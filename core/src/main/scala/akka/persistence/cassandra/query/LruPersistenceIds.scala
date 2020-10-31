/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.Props
import akka.actor.Timers
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator
import akka.cluster.ddata.SelfUniqueAddress
import akka.util.JavaDurationConverters._

object LruPersistenceIds extends ExtensionId[LruPersistenceIdsExt] with ExtensionIdProvider {
  type PersistenceId = String
  type Timestamp = Long

  def props(subscriber: ActorRef, predicate: LruPersistenceIds.PersistenceId => Boolean): Props = {
    Props(new LruPersistenceIds(subscriber, predicate))
  }

  final case class ActivePids(pids: Set[PersistenceId])
  final case class InactivePids(pids: Set[PersistenceId])

  private case object CleanupTick
  private final case class Cleanup(bucketKey: LWWMapKey[PersistenceId, Timestamp])

  val BucketKeyPrefix = "lruPids-"

  override def get(system: ActorSystem): LruPersistenceIdsExt = super.get(system)
  override def get(system: ClassicActorSystemProvider): LruPersistenceIdsExt = super.get(system)
  override def lookup = LruPersistenceIds
  override def createExtension(system: ExtendedActorSystem): LruPersistenceIdsExt =
    new LruPersistenceIdsExt(system)

  final case class Settings(numberOfBuckets: Int, maxEntriesPerBucket: Int, timeToLive: FiniteDuration) {
    val timeToLiveMillis: Long = timeToLive.toMillis
  }

}

class LruPersistenceIdsExt(system: ExtendedActorSystem) extends Extension {
  import LruPersistenceIds._

  implicit val node: SelfUniqueAddress = DistributedData(system).selfUniqueAddress

  // TODO might be good to have own Replicator
  val replicator: ActorRef = DistributedData(system).replicator

  val settings: Settings = {
    val cfg = system.settings.config.getConfig("all-events-query.lru-cache")
    val numberOfBuckets = cfg.getInt("number-of-buckets")
    val maxEntries = cfg.getInt("max-entries")
    val maxEntriesPerBucket = math.max(1, maxEntries / numberOfBuckets)
    val timeToLive = cfg.getDuration("time-to-live").asScala
    Settings(numberOfBuckets, maxEntriesPerBucket, timeToLive)
  }

  def touch(persistenceId: PersistenceId): Unit = {
    val i = math.abs(persistenceId.hashCode % settings.numberOfBuckets)
    val bucketKey = LWWMapKey[PersistenceId, Timestamp](s"$BucketKeyPrefix$i")
    replicator.tell(
      Replicator.Update(bucketKey, LWWMap.empty[PersistenceId, Timestamp], Replicator.WriteLocal) { bucket =>
        val now = System.currentTimeMillis()
        val timestamp = bucket.get(persistenceId) match {
          case None => now
          case Some(t) =>
            if (now > t) now
            else t + 1
        }
        bucket :+ persistenceId -> timestamp
      },
      system.provider.ignoreRef)
  }
}

class LruPersistenceIds(subscriber: ActorRef, predicate: LruPersistenceIds.PersistenceId => Boolean)
    extends Actor
    with Timers
    with ActorLogging {
  import LruPersistenceIds._

  // TODO we could implement a more special purpose ddata type instead of LWWMap

  private val lruPersistenceIdsExt = LruPersistenceIds(context.system)
  private implicit val node: SelfUniqueAddress = lruPersistenceIdsExt.node
  private val replicator = lruPersistenceIdsExt.replicator
  import lruPersistenceIdsExt.settings._

  private val cleanupInterval = timeToLive / 2

  private val bucketKeys =
    (0 until numberOfBuckets).map(i => LWWMapKey[PersistenceId, Timestamp](s"$BucketKeyPrefix$i")).toArray
  private var subscriberState: Map[LWWMapKey[PersistenceId, Timestamp], Map[PersistenceId, Timestamp]] =
    bucketKeys.map(_ -> Map.empty[PersistenceId, Timestamp]).toMap

  override def preStart(): Unit = {
    (0 until numberOfBuckets).foreach { i =>
      replicator ! Replicator.Subscribe(bucketKeys(i), self)
    }

    timers.startSingleTimer(
      CleanupTick,
      CleanupTick,
      timeToLive + ThreadLocalRandom.current().nextInt(cleanupInterval.toSeconds.toInt).seconds)
  }

  override def receive: Receive = {
    case c @ Replicator.Changed(key: LWWMapKey[PersistenceId, Timestamp]) =>
      val bucketEntries = c.get(key).entries
      val state = subscriberState(key)
      val updated = bucketEntries.filter {
        case (pid, timestamp) =>
          predicate(pid) && timestamp > state.getOrElse(pid, 0L)
      }
      val updatedState =
        if (updated.nonEmpty) {
          if (log.isDebugEnabled) // FIXME too much logging
            log.debug(
              "[{}] new active pids [{}] in bucket [{}]",
              updated.size,
              updated.keysIterator.mkString(", "),
              key.id)
          val s = state ++ updated
          subscriber ! ActivePids(updated.iterator.map(_._1).toSet)
          if (bucketEntries.size > maxEntriesPerBucket)
            scheduleCleanup(key)
          s
        } else {
          state
        }

      val removed = updatedState.keySet.diff(bucketEntries.keySet)
      if (removed.nonEmpty) {
        if (log.isDebugEnabled) // FIXME too much logging
          log.debug("[{}] new inactive pids [{}] in bucket [{}]", removed.size, removed.mkString(", "), key.id)
        subscriber ! InactivePids(removed)
        subscriberState = subscriberState.updated(key, updatedState -- removed)
      } else {
        subscriberState = subscriberState.updated(key, updatedState)
      }

    case CleanupTick =>
      bucketKeys.foreach { key =>
        scheduleCleanup(key)
      }
      if (!timers.isTimerActive(CleanupTick))
        timers.startTimerWithFixedDelay(CleanupTick, CleanupTick, cleanupInterval)

    case Cleanup(bucketKey) =>
      val now = System.currentTimeMillis()
      replicator ! Replicator.Update(bucketKey, LWWMap.empty[PersistenceId, Timestamp], Replicator.WriteLocal) {
        bucket =>
          // first remove those that have exceeded TTL
          val inactivePids = bucket.entries.collect {
            case (pid, timestamp) if now - timestamp > timeToLiveMillis => pid
          }
          val stillActive =
            if (inactivePids.isEmpty)
              bucket
            else {
              if (log.isDebugEnabled)
                log.debug("Evict [{}] inactive pids [{}]", inactivePids.size, inactivePids.mkString(","))
              inactivePids.foldLeft(bucket) {
                case (acc, pid) => acc.remove(node, pid)
              }
            }

          // then if still too many remove the oldest
          if (stillActive.size > maxEntriesPerBucket) {
            val oldest = stillActive.entries.toVector.sortBy(_._2).takeRight(stillActive.size - maxEntriesPerBucket)
            log.debug("Evict additional [{}] oldest pids [{}]", oldest.size, oldest.mkString(","))
            oldest.foldLeft(stillActive) {
              case (acc, (pid, _)) => acc.remove(node, pid)
            }
          } else {
            stillActive
          }
      }

  }

  private def scheduleCleanup(bucketKey: LWWMapKey[PersistenceId, Timestamp]): Unit = {
    val cleanupMsg = Cleanup(bucketKey)
    if (!timers.isTimerActive(cleanupMsg))
      timers.startSingleTimer(cleanupMsg, cleanupMsg, ThreadLocalRandom.current().nextInt(3).seconds)
  }

}
