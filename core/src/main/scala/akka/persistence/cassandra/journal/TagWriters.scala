/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.collection.immutable
import java.lang.{ Integer => JInt, Long => JLong }
import java.net.URLEncoder
import java.util.UUID

import akka.Done
import akka.actor.SupervisorStrategy.Escalate
import akka.pattern.ask
import akka.pattern.pipe
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.actor.Timers
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters._
import akka.util.Timeout
import com.datastax.driver.core.{ BatchStatement, BoundStatement, PreparedStatement, ResultSet, Statement }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.util.ByteString

@InternalApi private[akka] object TagWriters {

  private[akka] case class TagWritersSession(
      tagWritePs: () => Future[PreparedStatement],
      tagWriteWithMetaPs: () => Future[PreparedStatement],
      executeStatement: Statement => Future[Done],
      selectStatement: Statement => Future[ResultSet],
      tagProgressPs: () => Future[PreparedStatement],
      tagScanningPs: () => Future[PreparedStatement]) {

    def writeBatch(tag: Tag, events: Seq[(Serialized, Long)])(implicit ec: ExecutionContext): Future[Done] = {
      val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
      val tagWritePSs = for {
        withMeta <- tagWriteWithMetaPs()
        withoutMeta <- tagWritePs()
      } yield (withMeta, withoutMeta)

      tagWritePSs
        .map {
          case (withMeta, withoutMeta) =>
            events.foreach {
              case (event, pidTagSequenceNr) => {
                val ps = if (event.meta.isDefined) withMeta else withoutMeta
                val bound = ps.bind(
                  tag,
                  event.timeBucket.key: JLong,
                  event.timeUuid,
                  pidTagSequenceNr: JLong,
                  event.serialized,
                  event.eventAdapterManifest,
                  event.persistenceId,
                  event.sequenceNr: JLong,
                  event.serId: JInt,
                  event.serManifest,
                  event.writerUuid)
                event.meta.foreach { m =>
                  bound.setBytes("meta", m.serialized)
                  bound.setString("meta_ser_manifest", m.serManifest)
                  bound.setInt("meta_ser_id", m.serId)
                }
                batch.add(bound)
              }
            }
            batch
        }
        .flatMap(executeStatement)
    }

    def writeProgress(tag: Tag, persistenceId: String, seqNr: Long, tagPidSequenceNr: Long, offset: UUID)(
        implicit ec: ExecutionContext): Future[Done] = {
      tagProgressPs()
        .map(ps => ps.bind(persistenceId, tag, seqNr: JLong, tagPidSequenceNr: JLong, offset))
        .flatMap(executeStatement)
    }

  }

  private[akka] object BulkTagWrite {
    def apply(tw: TagWrite, owner: ActorRef): BulkTagWrite =
      BulkTagWrite(tw :: Nil, Nil)
  }

  /**
   * All tag writes should be for the same persistenceId
   */
  private[akka] case class BulkTagWrite(tagWrites: immutable.Seq[TagWrite], withoutTags: immutable.Seq[Serialized])
      extends NoSerializationVerificationNeeded

  /**
   * All serialised should be for the same persistenceId
   * @param actorRunning migration sends these messages without the actor running so TagWriters should not
   *                     validate that the pid is running
   */
  private[akka] case class TagWrite(tag: Tag, serialised: immutable.Seq[Serialized], actorRunning: Boolean = true)
      extends NoSerializationVerificationNeeded

  def props(settings: TagWriterSettings, tagWriterSession: TagWritersSession): Props =
    Props(new TagWriters(settings, tagWriterSession))

  final case class TagFlush(tag: String)
  final case class FlushAllTagWriters(timeout: Timeout)
  case object AllFlushed

  final case class SetTagProgress(pid: String, tagProgresses: Map[Tag, TagProgress])
  case object TagProcessAck

  final case class PersistentActorStarting(pid: String, persistentActor: ActorRef)
  case object PersistentActorStartingAck

  final case class TagWriteFailed(reason: Throwable)
  private case object WriteTagScanningTick
  private case class WriteTagScanningCompleted(result: Try[Done], startTime: Long, size: Int)

  private case class PersistentActorTerminated(pid: PersistenceId, ref: ActorRef)
}

/**
 * Manages all the tag writers.
 */
@InternalApi private[akka] class TagWriters(settings: TagWriterSettings, tagWriterSession: TagWritersSession)
    extends Actor
    with Timers
    with ActorLogging {

  import context.dispatcher

  // eager init and val because used from Future callbacks
  override val log = super.log

  // Escalate to the journal so it can stop
  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: Exception => Escalate
  }

  private var tagActors = Map.empty[String, ActorRef]
  // just used for local actor asks
  private implicit val timeout = Timeout(10.seconds)

  private var toBeWrittenScanning: Map[PersistenceId, SequenceNr] = Map.empty
  private var pendingScanning: Map[PersistenceId, SequenceNr] = Map.empty

  private var currentPersistentActors: Map[PersistenceId, ActorRef] = Map.empty

  // schedule as a single timer and trigger again once the writes are complete
  scheduleWriteTagScanningTick()

  def receive: Receive = {
    case FlushAllTagWriters(t) =>
      implicit val timeout: Timeout = t
      if (log.isDebugEnabled)
        log.debug("Flushing all tag writers [{}]", tagActors.keySet.mkString(", "))
      val replyTo = sender()
      val flushes = tagActors.map {
        case (tag, ref) =>
          (ref ? Flush)
            .mapTo[FlushComplete.type]
            .map(fc => {
              log.debug("Flush complete for tag {}", tag)
              fc
            })
      }
      Future.sequence(flushes).map(_ => AllFlushed).pipeTo(replyTo)
    case TagFlush(tag) =>
      tagActor(tag).tell(Flush, sender())
    case tw: TagWrite =>
      forwardTagWrite(tw)
    case BulkTagWrite(tws, withoutTags) =>
      tws.foreach(forwardTagWrite)
      updatePendingScanning(withoutTags)
    case WriteTagScanningTick =>
      writeTagScanning()

    case WriteTagScanningCompleted(result, startTime, size) =>
      scheduleWriteTagScanningTick()
      result match {
        case Success(_) =>
          log.debug(
            "Update tag scanning of [{}] pids took [{}] ms",
            size,
            (System.nanoTime() - startTime) / 1000 / 1000)
        case Failure(t) =>
          log.warning("Writing tag scanning failed. Reason {}", t)
      }

    case PersistentActorStarting(pid, persistentActor) =>
      // migration and journal specs can use dead letters as sender
      if (persistentActor != context.system.deadLetters) {
        currentPersistentActors.get(pid).foreach { ref =>
          log.warning(
            "Persistent actor starting for pid [{}]. Old ref hasn't terminated yet: [{}]. Persistent Actors with the same PersistenceId should not run concurrently",
            pid,
            ref)
        }
        currentPersistentActors += (pid -> persistentActor)
        log.debug("Watching pid [{}] actor [{}]", pid, persistentActor)
        context.watchWith(persistentActor, PersistentActorTerminated(pid, persistentActor))
      }
      sender() ! PersistentActorStartingAck

    case SetTagProgress(pid, tagProgresses: Map[Tag, TagProgress]) =>
      val missingProgress = tagActors.keySet -- tagProgresses.keySet
      log.debug(
        "Pid [{}] set tag progress [{}]. Tags to reset as not in progress: [{}]",
        pid,
        tagProgresses,
        missingProgress)

      val replyTo = sender()
      pendingScanning -= pid
      val tagWriterAcks = Future.sequence(tagProgresses.map {
        case (tag, progress) =>
          log.debug("Sending tag progress: [{}] [{}]", tag, progress)
          (tagActor(tag) ? ResetPersistenceId(tag, progress)).mapTo[ResetPersistenceIdComplete.type]
      })
      // We send an empty progress in case the tag actor has buffered events
      // and has never written any tag progress for this tag/pid
      val blankTagWriterAcks = Future.sequence(missingProgress.map { tag =>
        log.debug("Sending blank progress for tag [{}] pid [{}]", tag, pid)
        (tagActor(tag) ? ResetPersistenceId(tag, TagProgress(pid, 0, 0))).mapTo[ResetPersistenceIdComplete.type]
      })

      val recoveryNotificationComplete = for {
        _ <- tagWriterAcks
        _ <- blankTagWriterAcks
      } yield Done

      // if this fails (all local actor asks) the recovery will timeout
      recoveryNotificationComplete.foreach { _ =>
        replyTo ! TagProcessAck
      }

    case TagWriteFailed(_) =>
      toBeWrittenScanning = Map.empty
      pendingScanning = Map.empty

    case PersistentActorTerminated(pid, ref) =>
      currentPersistentActors.get(pid) match {
        case Some(currentRef) if currentRef == ref =>
          log.debug(
            "Persistent actor terminated [{}]. Informing TagWriter actors to drop state for pid: [{}]",
            ref,
            pid)
          tagActors.foreach {
            case (_, tagWriterRef) => tagWriterRef ! DropState(pid)
          }
          currentPersistentActors -= pid
        case Some(currentRef) =>
          log.debug(
            "Persistent actor terminated. However new actor ref for pid has been added. [{}]. Terminated ref: [{}] current ref: [{}]",
            pid,
            ref,
            currentRef)
        case None =>
          log.warning(
            "Unknown persistent actor terminated. Were multiple actors with the same PersistenceId running concurrently? Check warnings logs for this PersistenceId: [{}]. Ref: [{}]",
            pid,
            ref)
      }
  }

  private def forwardTagWrite(tw: TagWrite): Unit = {
    if (tw.actorRunning && !currentPersistentActors.contains(tw.serialised.head.persistenceId)) {
      log.warning(
        "received TagWrite but actor not active (dropping, will be resolved when actor restarts): [{}]",
        tw.serialised.head.persistenceId)
    } else {
      updatePendingScanning(tw.serialised)
      tagActor(tw.tag).forward(tw)
    }
  }

  private def updatePendingScanning(serialized: immutable.Seq[Serialized]): Unit = {
    serialized.foreach { ser =>
      pendingScanning.get(ser.persistenceId) match {
        case Some(seqNr) =>
          if (ser.sequenceNr > seqNr) // collect highest
            pendingScanning = pendingScanning.updated(ser.persistenceId, ser.sequenceNr)
        case None =>
          pendingScanning = pendingScanning.updated(ser.persistenceId, ser.sequenceNr)
      }
    }
  }

  private def writeTagScanning(): Unit = {

    val updates: Seq[(PersistenceId, SequenceNr)] = toBeWrittenScanning.toVector
    // current pendingScanning will be written on next tick, if no write failures
    toBeWrittenScanning = pendingScanning
    // collect new until next tick
    pendingScanning = Map.empty

    if (updates.nonEmpty) {

      if (log.isDebugEnabled) {
        val maxPrint = 20
        log.debug(
          "Update tag scanning [{}]",
          if (updates.size <= maxPrint) updates.take(maxPrint).mkString(",")
          else
            updates.take(maxPrint).mkString(",") + s" ...and ${updates.size - 20} more")
      }

      tagWriterSession.tagScanningPs().foreach { ps =>
        val startTime = System.nanoTime()

        def writeTagScanningBatch(group: Seq[(String, Long)]): Future[Done] = {
          val statements: Seq[BoundStatement] = group.map {
            case (pid, seqNr) => ps.bind(pid, seqNr: JLong)
          }
          Future.traverse(statements)(tagWriterSession.executeStatement).map(_ => Done)
        }

        // Execute 10 async statements at a time to not introduce too much load see issue #408.
        val batchIterator: Iterator[Seq[(PersistenceId, SequenceNr)]] = updates.grouped(10)

        var result = Future.successful[Done](Done)
        for (item <- batchIterator) {
          result = result.flatMap { _ =>
            writeTagScanningBatch(item)
          }
        }

        result.onComplete { result =>
          self ! WriteTagScanningCompleted(result, startTime, updates.size)
          result.failed.foreach(self ! TagWriteFailed(_))
        }
      }
    } else {
      scheduleWriteTagScanningTick()
    }
  }

  private def scheduleWriteTagScanningTick(): Unit = {
    timers.startSingleTimer(WriteTagScanningTick, WriteTagScanningTick, settings.scanningFlushInterval)
  }

  private def tagActor(tag: String): ActorRef =
    tagActors.get(tag) match {
      case None =>
        val ref = createTagWriter(tag)
        tagActors += (tag -> ref)
        ref
      case Some(ar) => ar
    }

  // protected for testing purposes
  protected def createTagWriter(tag: String): ActorRef = {
    context.actorOf(
      TagWriter.props(settings, tagWriterSession, tag).withDispatcher(context.props.dispatcher),
      name = URLEncoder.encode(tag, ByteString.UTF_8))
  }

}
