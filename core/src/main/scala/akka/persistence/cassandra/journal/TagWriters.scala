/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.collection.immutable
import java.lang.{ Integer => JInt, Long => JLong }
import java.net.URLEncoder
import java.util.UUID

import scala.concurrent.Promise
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
import akka.dispatch.ExecutionContexts
import akka.event.LoggingAdapter
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters._
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.util.ByteString
import akka.util.Timeout
import com.datastax.oss.driver.api.core.cql.{ BatchStatementBuilder, BatchType, BoundStatement, Statement }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TagWriters {

  private[akka] case class TagWritersSession(
      session: CassandraSession,
      writeProfile: String,
      readProfile: String,
      taggedPreparedStatements: TaggedPreparedStatements) {

    import taggedPreparedStatements._

    def executeWrite[T <: Statement[T]](stmt: Statement[T]): Future[Done] = {
      session.executeWrite(stmt.setExecutionProfileName(writeProfile))
    }

    def writeBatch(tag: Tag, events: Buffer)(implicit ec: ExecutionContext): Future[Done] = {
      val batch = new BatchStatementBuilder(BatchType.UNLOGGED)
      batch.setExecutionProfileName(writeProfile)
      val tagWritePSs = for {
        withMeta <- taggedPreparedStatements.WriteTagViewWithMeta
        withoutMeta <- taggedPreparedStatements.WriteTagViewWithoutMeta
      } yield (withMeta, withoutMeta)

      tagWritePSs
        .map {
          case (withMeta, withoutMeta) =>
            events.nextBatch.foreach { awaitingWrite =>
              awaitingWrite.events.foreach {
                case (event, pidTagSequenceNr) =>
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

                  val finished = event.meta match {
                    case Some(m) =>
                      bound
                        .setByteBuffer("meta", m.serialized)
                        .setString("meta_ser_manifest", m.serManifest)
                        .setInt("meta_ser_id", m.serId)
                    case None =>
                      bound
                  }

                  // this is a mutable builder
                  batch.addStatement(finished)
              }
            }
            batch.build()
        }
        .flatMap(executeWrite)
    }

    def writeProgress(tag: Tag, persistenceId: String, seqNr: Long, tagPidSequenceNr: Long, offset: UUID)(
        implicit ec: ExecutionContext): Future[Done] = {
      WriteTagProgress
        .map(
          ps =>
            ps.bind(persistenceId, tag, seqNr: JLong, tagPidSequenceNr: JLong, offset)
              .setExecutionProfileName(writeProfile))
        .flatMap(executeWrite)
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
   *
   * @param actorRunning migration sends these messages without the actor running so TagWriters should not
   *                     validate that the pid is running
   */
  private[akka] case class TagWrite(tag: Tag, serialised: immutable.Seq[Serialized], actorRunning: Boolean = true)
      extends NoSerializationVerificationNeeded

  def props(settings: TagWriterSettings, tagWriterSession: TagWritersSession): Props =
    Props(new TagWriters(settings, tagWriterSession))

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

  private case class TagWriterTerminated(tag: String)

  /**
   * @param message   the message to send
   */
  private case class PassivateBufferEntry(message: Any, response: Promise[Any])

}

/**
 * INTERNAL API
 * Manages all the tag writers.
 */
@InternalApi private[akka] class TagWriters(settings: TagWriterSettings, tagWriterSession: TagWritersSession)
    extends Actor
    with Timers
    with ActorLogging {

  import context.dispatcher

  // eager init and val because used from Future callbacks
  override val log: LoggingAdapter = super.log

  // Escalate to the journal so it can stop
  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: Exception => Escalate
  }

  private var tagActors = Map.empty[String, ActorRef]
  // When TagWriter is idle it starts passivation process. Incoming messages are buffered here.
  private var passivatingTagActors = Map.empty[String, Vector[PassivateBufferEntry]]
  // just used for local actor asks
  private implicit val timeout: Timeout = Timeout(10.seconds)

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
      val flushes = tagActors.keySet.map { tag =>
        askTagActor(tag, Flush)
          .mapTo[FlushComplete.type]
          .map(fc => {
            log.debug("Flush complete for tag {}", tag)
            fc
          })
      }
      Future.sequence(flushes).map(_ => AllFlushed).pipeTo(replyTo)
    case tw: TagWrite =>
      // this only comes from the replay, an ack is not required right now.
      forwardTagWrite(tw).pipeTo(sender())
    case BulkTagWrite(tws, withoutTags) =>
      val replyTo = sender()
      val forwards = tws.map(forwardTagWrite)
      Future.sequence(forwards).map(_ => Done)(ExecutionContexts.parasitic).pipeTo(replyTo)
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
          askTagActor(tag, ResetPersistenceId(tag, progress)).mapTo[ResetPersistenceIdComplete.type]
      })
      // We send an empty progress in case the tag actor has buffered events
      // and has never written any tag progress for this tag/pid
      val blankTagWriterAcks = Future.sequence(missingProgress.map { tag =>
        log.debug("Sending blank progress for tag [{}] pid [{}]", tag, pid)
        askTagActor(tag, ResetPersistenceId(tag, TagProgress(pid, 0, 0))).mapTo[ResetPersistenceIdComplete.type]
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

    case PassivateTagWriter(tag) =>
      tagActors.get(tag) match {
        case Some(tagWriter) =>
          if (!passivatingTagActors.contains(tag))
            passivatingTagActors = passivatingTagActors.updated(tag, Vector.empty)
          log.debug("Tag writer {} for tag [{}] is passivating", tagWriter, tag)
          tagWriter ! StopTagWriter
        case None =>
          log.warning(
            "Unknown tag [{}] in passivate request from {}. Please raise an issue with debug logs.",
            tag,
            sender())
      }

    case CancelPassivateTagWriter(tag) =>
      passivatingTagActors.get(tag).foreach { buffer =>
        passivatingTagActors = passivatingTagActors - tag
        log.debug("Tag writer {} for tag [{}] canceled passivation.", sender(), tag)
        sendPassivateBuffer(tag, buffer)
      }

    case TagWriterTerminated(tag) =>
      tagWriterTerminated(tag)

  }

  private def forwardTagWrite(tw: TagWrite): Future[Done] = {
    if (tw.actorRunning && !currentPersistentActors.contains(tw.serialised.head.persistenceId)) {
      log.warning(
        "received TagWrite but actor not active (dropping, will be resolved when actor restarts): [{}]",
        tw.serialised.head.persistenceId)
      Future.successful(Done)
    } else {
      updatePendingScanning(tw.serialised)
      askTagActor(tw.tag, tw).map(_ => Done)(ExecutionContexts.parasitic)
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

      tagWriterSession.taggedPreparedStatements.WriteTagScanning.foreach { ps =>
        val startTime = System.nanoTime()

        def writeTagScanningBatch(group: Seq[(String, Long)]): Future[Done] = {
          val statements: Seq[BoundStatement] = group.map {
            case (pid, seqNr) => ps.bind(pid, seqNr: JLong)
          }
          Future.traverse(statements)(tagWriterSession.executeWrite).map(_ => Done)
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
    context.watchWith(
      context.actorOf(
        TagWriter.props(settings, tagWriterSession, tag, self).withDispatcher(context.props.dispatcher),
        name = URLEncoder.encode(tag, ByteString.UTF_8)),
      TagWriterTerminated(tag))
  }

  private def askTagActor(tag: String, message: Any)(implicit timeout: Timeout): Future[Any] = {
    passivatingTagActors.get(tag) match {
      case Some(buffer) =>
        val p = Promise[Any]()
        passivatingTagActors = passivatingTagActors.updated(tag, buffer :+ PassivateBufferEntry(message, p))
        p.future
      case None =>
        tagActor(tag).ask(message)
    }
  }

  private def tagWriterTerminated(tag: String): Unit = {
    tagActors.get(tag) match {
      case Some(ref) =>
        passivatingTagActors.get(tag) match {
          case Some(buffer) =>
            tagActors = tagActors - tag
            passivatingTagActors = passivatingTagActors - tag
            if (buffer.isEmpty)
              log.debug("Tag writer {} for tag [{}] terminated after passivation.", ref, tag)
            else {
              log.debug(
                "Tag writer {} for tag [{}] terminated after passivation, but starting again " +
                "because [{}] messages buffered.",
                ref,
                tag,
                buffer.size)
              sendPassivateBuffer(tag, buffer)
            }
          case None =>
            log.warning(
              "Tag writer {} for tag [{}] terminated without passivation. Please raise an issue with debug logs.",
              ref,
              tag)
            tagActors = tagActors - tag
        }
      case None =>
        log.warning("Unknown tag writer for tag [{}] terminated. Please raise an issue with debug logs.", tag)
    }
  }

  private def sendPassivateBuffer(tag: String, buffer: Vector[PassivateBufferEntry]): Unit = {
    buffer.foreach { entry =>
      entry.response.completeWith(askTagActor(tag, entry.message))
    }
  }
}
