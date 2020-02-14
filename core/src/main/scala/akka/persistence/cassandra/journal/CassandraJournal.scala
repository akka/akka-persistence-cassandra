/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.util.Collections
import java.util.{ UUID, HashMap => JHMap, Map => JMap }

import akka.Done
import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.annotation.InternalApi
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.alpakka.cassandra._
import akka.persistence._
import akka.persistence.cassandra.EventWithMetaData.UnknownMetaData
import akka.persistence.cassandra._
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.journal.{ AsyncWriteJournal, Tagged }
import akka.persistence.query.PersistenceQuery
import akka.persistence.cassandra.journal.TagWriters.{ BulkTagWrite, TagWrite, TagWritersSession }
import akka.persistence.cassandra.journal.TagWriter.TagProgress
import akka.serialization.{ AsyncSerializer, Serialization, SerializationExtension }
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraSession, CassandraSessionRegistry }
import akka.stream.scaladsl.Sink
import akka.util.OptionVal
import com.datastax.oss.driver.api.core.cql._
import com.typesafe.config.Config
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.oss.protocol.internal.util.Bytes
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.compat.java8.FutureConverters._

import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory

/**
 * INTERNAL API
 *
 * Journal implementation of the cassandra plugin.
 */
@InternalApi private[akka] final class CassandraJournal(journalConfig: Config, cfgPath: String)
    extends AsyncWriteJournal
    with NoSerializationVerificationNeeded {
  import CassandraJournal._
  import context.system

  // shared config is one level above the journal specific
  private val sharedConfigPath = cfgPath.replaceAll("""\.journal$""", "")
  private val fullConfig =
    ConfigFactory
      .parseMap(Collections.singletonMap(cfgPath, journalConfig.root()))
      .withFallback(context.system.settings.config)
  private val sharedConfig = fullConfig.getConfig(sharedConfigPath)
  private val settings = new PluginSettings(context.system, sharedConfig)
  import settings._
  private val eventDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(context.system)

  private val statements: CassandraStatements = new CassandraStatements(settings)
  private val serialization = SerializationExtension(context.system)
  private val log: LoggingAdapter = Logging(context.system, getClass)

  private implicit val ec: ExecutionContext = context.dispatcher

  // readHighestSequence must be performed after pending write for a persistenceId
  // when the persistent actor is restarted.
  // It seems like C* doesn't support session consistency so we handle it ourselves.
  // https://aphyr.com/posts/299-the-trouble-with-timestamps
  private val writeInProgress: JMap[String, Future[Done]] = new JHMap

  // Can't think of a reason why we can't have writes and deletes
  // run concurrently. This should be a very infrequently used
  // so fine to use an immutable list as the value
  private val pendingDeletes: JMap[String, List[PendingDelete]] = new JHMap

  private val session: CassandraSession = CassandraSessionRegistry(context.system)
    .sessionFor(sharedConfigPath, context.dispatcher, ses => statements.executeAllCreateKeyspaceAndTables(ses))

  private val taggedPreparedStatements = new TaggedPreparedStatements(statements.journalStatements, session.prepare)
  private val tagRecovery = new CassandraTagRecovery(context.system, session, settings, taggedPreparedStatements)
  private val tagWriterSession = TagWritersSession(
    session,
    settings.journalSettings.writeProfile,
    settings.journalSettings.readProfile,
    taggedPreparedStatements)
  private val tagWrites: Option[ActorRef] =
    if (settings.eventsByTagSettings.eventsByTagEnabled)
      Some(
        context.actorOf(
          TagWriters
            .props(settings.eventsByTagSettings.tagWriterSettings, tagWriterSession)
            .withDispatcher(context.props.dispatcher),
          "tagWrites"))
    else None

  private def preparedWriteMessage =
    session.prepare(statements.journalStatements.writeMessage(withMeta = false))
  private def preparedSelectDeletedTo: Option[Future[PreparedStatement]] = {
    if (settings.journalSettings.supportDeletes)
      Some(session.prepare(statements.journalStatements.selectDeletedTo))
    else
      None
  }
  private def preparedSelectHighestSequenceNr: Future[PreparedStatement] =
    session.prepare(statements.journalStatements.selectHighestSequenceNr)

  private def deletesNotSupportedException: Future[PreparedStatement] =
    Future.failed(new IllegalArgumentException(s"Deletes not supported because config support-deletes=off"))

  private def preparedInsertDeletedTo: Future[PreparedStatement] = {
    if (settings.journalSettings.supportDeletes)
      session.prepare(statements.journalStatements.insertDeletedTo)
    else
      deletesNotSupportedException
  }
  private def preparedDeleteMessages: Future[PreparedStatement] = {
    if (settings.journalSettings.supportDeletes)
      session.prepare(statements.journalStatements.deleteMessages)
    else
      deletesNotSupportedException
  }

  private def preparedWriteMessageWithMeta =
    session.prepare(statements.journalStatements.writeMessage(withMeta = true))
  private def preparedSelectMessages =
    session.prepare(statements.journalStatements.selectMessages)

  private lazy val queries: CassandraReadJournal =
    PersistenceQuery(context.system.asInstanceOf[ExtendedActorSystem])
      .readJournalFor[CassandraReadJournal](s"$sharedConfigPath.query", fullConfig)

  // For TagWriters/TagWriter children
  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception =>
      log.error(e, "Cassandra Journal has experienced an unexpected error and requires an ActorSystem restart.")
      if (settings.journalSettings.coordinatedShutdownOnError) {
        CoordinatedShutdown(context.system).run(CassandraJournalUnexpectedError)
      }
      context.stop(context.self)
      Stop
  }

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! CassandraJournal.Init
  }

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, f) =>
      writeInProgress.remove(persistenceId, f)

    case DeleteFinished(persistenceId, toSequenceNr, result) =>
      log.debug("Delete finished for persistence id [{}] to [{}] result [{}]", persistenceId, toSequenceNr, result)
      pendingDeletes.get(persistenceId) match {
        case null =>
          log.error(
            "Delete finished but not in pending. Please raise a bug with logs. PersistenceId: [{}]",
            persistenceId)
        case Nil =>
          log.error(
            "Delete finished but not in pending (empty). Please raise a bug with logs. PersistenceId: [{}]",
            persistenceId)
        case current :: tail =>
          current.p.complete(result)
          tail match {
            case Nil =>
              pendingDeletes.remove(persistenceId)
            case next :: _ =>
              pendingDeletes.put(persistenceId, tail)
              delete(next.pid, next.toSequenceNr)
          }
      }

    case CassandraJournal.Init =>
      // try initialize early, to be prepared for first real request
      preparedWriteMessage
      preparedWriteMessageWithMeta
      preparedSelectMessages
      preparedSelectHighestSequenceNr
      if (settings.journalSettings.supportDeletes) {
        preparedDeleteMessages
        preparedSelectDeletedTo
        preparedInsertDeletedTo
      }
      queries.initialize()

      if (settings.eventsByTagSettings.eventsByTagEnabled) {
        taggedPreparedStatements.init()
      }
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    //
    // We must NOT catch serialization exceptions here because rejections will cause
    // holes in the sequence number series and we use the sequence numbers to detect
    // missing (delayed) events in the eventByTag query.
    //
    // Note that we assume that all messages have the same persistenceId, which is
    // the case for Akka 2.4.2.
    def serialize(aw: Seq[(PersistentRepr, UUID)]): Future[SerializedAtomicWrite] = {
      val serializedEventsFut: Future[Seq[Serialized]] =
        Future.sequence(aw.map {
          case (pr, uuid) =>
            val (pr2, tags) = pr.payload match {
              case Tagged(payload, ts) =>
                (pr.withPayload(payload), ts)
              case _ =>
                (pr, Set.empty[String])
            }
            serializeEvent(pr2, tags, uuid, settings.eventsByTagSettings.bucketSize, serialization, context.system)
        })

      serializedEventsFut.map { serializedEvents =>
        SerializedAtomicWrite(aw.head._1.persistenceId, serializedEvents)
      }
    }

    val writesWithUuids: Seq[Seq[(PersistentRepr, UUID)]] =
      messages.map(aw => aw.payload.map(pr => (pr, generateUUID(pr))))

    val writeInProgressForPersistentId = Promise[Done]
    val pid = messages.head.persistenceId
    writeInProgress.put(pid, writeInProgressForPersistentId.future)

    val toReturn: Future[Nil.type] = Future.sequence(writesWithUuids.map(w => serialize(w))).flatMap {
      serialized: Seq[SerializedAtomicWrite] =>
        val result: Future[Any] =
          if (messages.map(_.payload.size).sum <= journalSettings.maxMessageBatchSize) {
            // optimize for the common case
            writeMessages(serialized)
          } else {

            //if presistAll was used, single AtomicWrite can already contain complete batch, so we need to regroup writes correctly
            val groups: List[List[SerializedAtomicWrite]] = groupedWrites(serialized.toList.reverse, Nil, Nil)

            // execute the groups in sequence
            def rec(todo: List[List[SerializedAtomicWrite]]): Future[Any] =
              todo match {
                case write :: remainder =>
                  writeMessages(write).flatMap(_ => rec(remainder))
                case Nil => Future.successful(())
              }

            rec(groups)
          }
        result.map { _ =>
          tagWrites.foreach(_ ! extractTagWrites(serialized))
          Nil
        }

    }

    // if the write fails still need to remove state from the map
    toReturn.onComplete { _ =>
      sendWriteFinished(pid, writeInProgressForPersistentId)
    }

    toReturn
  }

  //Regroup batches by payload size
  @tailrec
  private def groupedWrites(
      reversed: List[SerializedAtomicWrite],
      currentGroup: List[SerializedAtomicWrite],
      grouped: List[List[SerializedAtomicWrite]]): List[List[SerializedAtomicWrite]] = reversed match {
    case Nil => currentGroup +: grouped
    case x :: xs if currentGroup.size + x.payload.size < journalSettings.maxMessageBatchSize =>
      groupedWrites(xs, x +: currentGroup, grouped)
    case x :: xs => groupedWrites(xs, List(x), currentGroup +: grouped)
  }

  private def sendWriteFinished(pid: String, writeInProgressForPid: Promise[Done]): Unit = {
    self ! WriteFinished(pid, writeInProgressForPid.future)
    writeInProgressForPid.success(Done)
  }

  /**
   * UUID generation is deliberately externalized to allow subclasses to customize the time based uuid for special cases.
   * see https://discuss.lightbend.com/t/akka-persistence-cassandra-events-by-tags-bucket-size-based-on-time-vs-burst-load/1411 and make sure you understand the risk of doing this wrong.
   */
  protected def generateUUID(pr: PersistentRepr): UUID = Uuids.timeBased()

  private def extractTagWrites(serialized: Seq[SerializedAtomicWrite]): BulkTagWrite = {
    if (serialized.isEmpty) BulkTagWrite(Nil, Nil)
    else if (serialized.size == 1 && serialized.head.payload.size == 1) {
      // optimization for one single event, which is the typical case
      val s = serialized.head.payload.head
      if (s.tags.isEmpty) BulkTagWrite(Nil, s :: Nil)
      else BulkTagWrite(s.tags.map(tag => TagWrite(tag, s :: Nil)).toList, Nil)
    } else {
      val messagesByTag: Map[String, Seq[Serialized]] =
        serialized.flatMap(_.payload).flatMap(s => s.tags.map((_, s))).groupBy(_._1).map {
          case (tag, messages) => (tag, messages.map(_._2))
        }
      val messagesWithoutTag =
        for {
          a <- serialized
          b <- a.payload
          if b.tags.isEmpty
        } yield b

      val writesWithTags: immutable.Seq[TagWrite] = messagesByTag.map {
        case (tag, writes) => TagWrite(tag, writes)
      }.toList

      BulkTagWrite(writesWithTags, messagesWithoutTag)
    }

  }

  private def writeMessages(atomicWrites: Seq[SerializedAtomicWrite]): Future[Unit] = {
    val boundStatements: Seq[Future[BoundStatement]] = statementGroup(atomicWrites)
    boundStatements.size match {
      case 1 =>
        boundStatements.head.flatMap(execute(_))
      case 0 => Future.successful(())
      case _ =>
        Future.sequence(boundStatements).flatMap { stmts =>
          executeBatch(batch => stmts.foldLeft(batch) { case (acc, next) => acc.add(next) })
        }
    }
  }

  private def statementGroup(atomicWrites: Seq[SerializedAtomicWrite]): Seq[Future[BoundStatement]] = {
    val maxPnr = partitionNr(atomicWrites.last.payload.last.sequenceNr, journalSettings.targetPartitionSize)
    val firstSeq = atomicWrites.head.payload.head.sequenceNr
    val minPnr = partitionNr(firstSeq, journalSettings.targetPartitionSize)
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(
      maxPnr - minPnr <= 1,
      "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[Future[BoundStatement]] = all.map { m: Serialized =>
      // using two separate statements with or without the meta data columns because
      // then users doesn't have to alter table and add the new columns if they don't use
      // the meta data feature
      val stmt =
        if (m.meta.isDefined) preparedWriteMessageWithMeta
        else preparedWriteMessage

      stmt.map { stmt =>
        val bs = stmt
          .bind()
          .setString("persistence_id", persistenceId)
          .setLong("partition_nr", maxPnr)
          .setLong("sequence_nr", m.sequenceNr)
          .setUuid("timestamp", m.timeUuid)
          // Keeping as text for backward compatibility
          .setString("timebucket", m.timeBucket.key.toString)
          .setString("writer_uuid", m.writerUuid)
          .setInt("ser_id", m.serId)
          .setString("ser_manifest", m.serManifest)
          .setString("event_manifest", m.eventAdapterManifest)
          .setByteBuffer("event", m.serialized)
          .setSet("tags", m.tags.asJava, classOf[String])

        // meta data, if any
        m.meta
          .map(meta => {
            bs.setInt("meta_ser_id", meta.serId)
              .setString("meta_ser_manifest", meta.serManifest)
              .setByteBuffer("meta", meta.serialized)
          })
          .getOrElse(bs)
      }
    }

    writes
  }

  /**
   * It is assumed that this is only called during a replay and if fromSequenceNr == highest
   * then asyncReplayMessages won't be called. In that case the tag progress is updated
   * in here rather than during replay messages.
   */
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("[{}] asyncReadHighestSequenceNr [{}] [{}]", persistenceId, fromSequenceNr, sender())
    val highestSequenceNr = writeInProgress.get(persistenceId) match {
      case null =>
        asyncReadHighestSequenceNrInternal(persistenceId, fromSequenceNr)
      case f =>
        f.flatMap(_ => asyncReadHighestSequenceNrInternal(persistenceId, fromSequenceNr))
    }

    val toReturn = if (settings.eventsByTagSettings.eventsByTagEnabled) {

      // This relies on asyncReadHighestSequenceNr having the correct sender()
      // No other calls into the async journal have this as they are called from Future callbacks
      val persistentActor = sender()

      for {
        seqNr <- highestSequenceNr
        _ <- tagRecovery.sendPersistentActorStarting(persistenceId, persistentActor, tagWrites.get)
        _ <- if (seqNr == fromSequenceNr && seqNr != 0) {
          log.debug("[{}] snapshot is current so replay won't be required. Calculating tag progress now", persistenceId)
          val scanningSeqNrFut = tagRecovery.tagScanningStartingSequenceNr(persistenceId)
          for {
            tp <- tagRecovery.lookupTagProgress(persistenceId)
            _ <- tagRecovery.setTagProgress(persistenceId, tp, tagWrites.get)
            scanningSeqNr <- scanningSeqNrFut
            _ <- sendPreSnapshotTagWrites(scanningSeqNr, fromSequenceNr, persistenceId, Long.MaxValue, tp)
          } yield seqNr
        } else if (seqNr == 0) {
          log.debug("[{}] New pid. Sending blank tag progress. [{}]", persistenceId, persistentActor)
          tagRecovery.setTagProgress(persistenceId, Map.empty, tagWrites.get)
        } else {
          Future.successful(())
        }
      } yield seqNr
    } else {
      highestSequenceNr
    }

    toReturn.onComplete { highestSeq =>
      log.debug("asyncReadHighestSequenceNr {} returning {}", persistenceId, highestSeq)
    }

    toReturn
  }

  private def asyncReadHighestSequenceNrInternal(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap { h =>
      asyncFindHighestSequenceNr(
        persistenceId,
        math.max(fromSequenceNr, h),
        settings.journalSettings.targetPartitionSize)
    }
  }

  /**
   * Not thread safe. Assumed to only be called from the journal actor.
   * However, unlike asyncWriteMessages it can be called before the previous Future completes
   */
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {

    // TODO could "optimize" away deletes that overlap?
    pendingDeletes.get(persistenceId) match {
      case null =>
        log.debug("[{}] No outstanding delete. Sequence nr [{}]", persistenceId, toSequenceNr)
        // fast path, no outstanding deletes for this persistenceId
        val p = Promise[Unit]()
        pendingDeletes.put(persistenceId, List(PendingDelete(persistenceId, toSequenceNr, p)))
        delete(persistenceId, toSequenceNr)
        p.future
      case otherDeletes =>
        if (otherDeletes.length > settings.journalSettings.maxConcurrentDeletes) {
          log.error(
            "[}}] Over [{}] outstanding deletes. Failing delete",
            persistenceId,
            settings.journalSettings.maxConcurrentDeletes)
          Future.failed(new RuntimeException(
            s"Over ${settings.journalSettings.maxConcurrentDeletes} outstanding deletes for persistenceId $persistenceId"))
        } else {
          log.debug(
            "[{}] outstanding delete. Delete to seqNr [{}] will be scheduled after previous one finished.",
            persistenceId,
            toSequenceNr)
          val p = Promise[Unit]()
          pendingDeletes.put(persistenceId, otherDeletes :+ PendingDelete(persistenceId, toSequenceNr, p))
          p.future
        }
    }
  }

  private def delete(persistenceId: String, toSequenceNr: Long): Future[Unit] = {

    def physicalDelete(lowestPartition: Long, highestPartition: Long, toSeqNr: Long): Future[Done] = {
      if (settings.cassandra2xCompat) {
        def asyncDeleteMessages(partitionNr: Long, messageIds: Seq[MessageId]): Future[Unit] = {
          val boundStatements = messageIds.map(mid =>
            preparedDeleteMessages.map(_.bind(mid.persistenceId, partitionNr: JLong, mid.sequenceNr: JLong)))
          Future.sequence(boundStatements).flatMap { stmts =>
            executeBatch(batch => stmts.foldLeft(batch) { case (acc, next) => acc.add(next) })
          }
        }

        val partitionInfos = (lowestPartition to highestPartition).map(partitionInfo(persistenceId, _, toSeqNr))
        val deleteResult =
          Future.sequence(partitionInfos.map(future =>
            future.flatMap(pi => {
              Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(journalSettings.maxMessageBatchSize).map {
                group =>
                  {
                    val groupDeleteResult =
                      asyncDeleteMessages(pi.partitionNr, group.map(MessageId(persistenceId, _)))
                    groupDeleteResult.failed.foreach { e =>
                      log.warning(
                        s"Unable to complete deletes for persistence id {}, toSequenceNr {}. " +
                        "The plugin will continue to function correctly but you will need to manually delete the old messages. " +
                        "Caused by: [{}: {}]",
                        persistenceId,
                        toSequenceNr,
                        e.getClass.getName,
                        e.getMessage)
                    }
                    groupDeleteResult
                  }
              })
            })))
        deleteResult.map(_ => Done)

      } else {
        val deleteResult =
          Future.sequence((lowestPartition to highestPartition).map { partitionNr =>
            val boundDeleteMessages =
              preparedDeleteMessages.map(_.bind(persistenceId, partitionNr: JLong, toSeqNr: JLong))
            boundDeleteMessages.flatMap(execute(_))
          })
        deleteResult.failed.foreach { e =>
          log.warning(
            "Unable to complete deletes for persistence id {}, toSequenceNr {}. " +
            "The plugin will continue to function correctly but you will need to manually delete the old messages. " +
            "Caused by: [{}: {}]",
            persistenceId,
            toSequenceNr,
            e.getClass.getName,
            e.getMessage)
        }
        deleteResult.map(_ => Done)
      }
    }

    /**
     * Deletes the events by inserting into the metadata table deleted_to
     * and physically deletes the rows.
     */
    def logicalAndPhysicalDelete(highestDeletedSequenceNumber: Long, highestSequenceNr: Long): Future[Done] = {
      val lowestPartition = partitionNr(highestDeletedSequenceNumber + 1, journalSettings.targetPartitionSize)
      val toSeqNr = math.min(toSequenceNr, highestSequenceNr)
      val highestPartition = partitionNr(toSeqNr, journalSettings.targetPartitionSize) + 1 // may have been moved to the next partition
      val logicalDelete =
        if (toSeqNr <= highestDeletedSequenceNumber) {
          // already deleted same or higher sequence number, don't update highestDeletedSequenceNumber,
          // but perform the physical delete (again), may be a retry request
          Future.successful(())
        } else {
          val boundInsertDeletedTo =
            preparedInsertDeletedTo.map(_.bind(persistenceId, toSeqNr: JLong))
          boundInsertDeletedTo.flatMap(execute)
        }
      logicalDelete.flatMap(_ => physicalDelete(lowestPartition, highestPartition, toSeqNr))
    }

    val deleteResult = for {
      highestDeletedSequenceNumber <- asyncHighestDeletedSequenceNumber(persistenceId)
      highestSequenceNr <- {
        // MaxValue may be used as magic value to delete all events without specifying actual toSequenceNr
        if (toSequenceNr == Long.MaxValue)
          asyncFindHighestSequenceNr(persistenceId, highestDeletedSequenceNumber, journalSettings.targetPartitionSize)
        else
          Future.successful(toSequenceNr)
      }
      _ <- logicalAndPhysicalDelete(highestDeletedSequenceNumber, highestSequenceNr)
    } yield ()

    // Kick off any pending deletes when finished.
    deleteResult.onComplete { result =>
      self ! DeleteFinished(persistenceId, toSequenceNr, result)
    }

    deleteResult
  }

  private def partitionInfo(persistenceId: String, partitionNr: Long, maxSequenceNr: Long): Future[PartitionInfo] = {
    val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, partitionNr: JLong))
    boundSelectHighestSequenceNr
      .flatMap(selectOne)
      .map(
        row =>
          row
            .map(s =>
              PartitionInfo(partitionNr, minSequenceNr(partitionNr), math.min(s.getLong("sequence_nr"), maxSequenceNr)))
            .getOrElse(PartitionInfo(partitionNr, minSequenceNr(partitionNr), -1)))
  }

  private def asyncHighestDeletedSequenceNumber(persistenceId: String): Future[Long] = {
    preparedSelectDeletedTo match {
      case Some(pstmt) =>
        val boundSelectDeletedTo = pstmt.map(_.bind(persistenceId))
        boundSelectDeletedTo.flatMap(selectOne).map(rowOption => rowOption.map(_.getLong("deleted_to")).getOrElse(0))
      case None =>
        Future.successful(0L)
    }
  }

  private def asyncFindHighestSequenceNr(
      persistenceId: String,
      fromSequenceNr: Long,
      partitionSize: Long): Future[Long] = {
    def find(currentPnr: Long, currentSnr: Long, foundEmptyPartition: Boolean): Future[Long] = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(ps => {
        val bound = ps.bind(persistenceId, currentPnr: JLong)
        bound

      })
      boundSelectHighestSequenceNr
        .flatMap(selectOne)
        .map { rowOption =>
          rowOption.map(_.getLong("sequence_nr"))
        }
        .flatMap {
          case None | Some(0) =>
            // never been to this partition, query one more partition because AtomicWrite can span (skip)
            // one entire partition
            // Some(0) when old schema with static used column, everything deleted in this partition
            if (foundEmptyPartition) Future.successful(currentSnr)
            else find(currentPnr + 1, currentSnr, foundEmptyPartition = true)
          case Some(nextHighest) =>
            find(currentPnr + 1, nextHighest, foundEmptyPartition = false)
        }
    }

    find(partitionNr(fromSequenceNr, partitionSize), fromSequenceNr, foundEmptyPartition = false)
  }

  private def executeBatch(body: BatchStatement => BatchStatement): Future[Unit] = {
    var batch =
      new BatchStatementBuilder(BatchType.UNLOGGED).build().setExecutionProfileName(journalSettings.writeProfile)
    batch = body(batch)
    session.underlying().flatMap(_.executeAsync(batch).toScala).map(_ => ())
  }

  private def selectOne[T <: Statement[T]](stmt: Statement[T]): Future[Option[Row]] = {
    session.selectOne(stmt.setExecutionProfileName(journalSettings.readProfile))
  }

  private def minSequenceNr(partitionNr: Long): Long =
    partitionNr * journalSettings.targetPartitionSize + 1

  private def execute[T <: Statement[T]](stmt: Statement[T]): Future[Unit] = {
    session.executeWrite(stmt.setExecutionProfileName(journalSettings.writeProfile)).map(_ => ())
  }

  // TODO this serialises and re-serialises the messages for fixing tag_views
  // Could have an events by persistenceId stage that has the raw payload
  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug("[{}] asyncReplayMessages from [{}] to [{}]", persistenceId, fromSequenceNr, toSequenceNr)

    if (eventsByTagSettings.eventsByTagEnabled) {

      val recoveryPrep: Future[Map[String, TagProgress]] = {
        val scanningSeqNrFut = tagRecovery.tagScanningStartingSequenceNr(persistenceId)
        for {
          tp <- tagRecovery.lookupTagProgress(persistenceId)
          _ <- tagRecovery.setTagProgress(persistenceId, tp, tagWrites.get)
          scanningSeqNr <- scanningSeqNrFut
          _ <- sendPreSnapshotTagWrites(scanningSeqNr, fromSequenceNr, persistenceId, max, tp)
        } yield tp
      }

      Source
        .futureSource(recoveryPrep.map((tp: Map[Tag, TagProgress]) => {
          log.debug(
            "[{}] starting recovery with tag progress: [{}]. From [{}] to [{}]",
            persistenceId,
            tp,
            fromSequenceNr,
            toSequenceNr)
          queries
            .eventsByPersistenceId(
              persistenceId,
              fromSequenceNr,
              toSequenceNr,
              max,
              None,
              settings.journalSettings.readProfile,
              "asyncReplayMessages",
              extractor = Extractors.taggedPersistentRepr(eventDeserializer, serialization))
            .mapAsync(1)(tagRecovery.sendMissingTagWrite(tp, tagWrites.get))
        }))
        .map(te => queries.mapEvent(te.pr))
        .runForeach(replayCallback)
        .map(_ => ())

    } else {
      queries
        .eventsByPersistenceId(
          persistenceId,
          fromSequenceNr,
          toSequenceNr,
          max,
          None,
          settings.journalSettings.readProfile,
          "asyncReplayMessages",
          extractor = Extractors.persistentRepr(eventDeserializer, serialization))
        .map(p => queries.mapEvent(p.persistentRepr))
        .runForeach(replayCallback)
        .map(_ => ())
    }
  }

  private def sendPreSnapshotTagWrites(
      minProgressNr: Long,
      fromSequenceNr: Long,
      pid: String,
      max: Long,
      tp: Map[Tag, TagProgress]): Future[Done] = {
    if (minProgressNr < fromSequenceNr) {
      val scanTo = fromSequenceNr - 1
      log.debug(
        "[{}], Scanning events before snapshot to recover tag_views: From: [{}] to: [{}]",
        pid,
        minProgressNr,
        scanTo)
      queries
        .eventsByPersistenceId(
          pid,
          minProgressNr,
          scanTo,
          max,
          None,
          settings.journalSettings.readProfile,
          "asyncReplayMessagesPreSnapshot",
          Extractors.optionalTaggedPersistentRepr(eventDeserializer, serialization))
        .mapAsync(1) { t =>
          t.tagged match {
            case OptionVal.Some(tpr) =>
              tagRecovery.sendMissingTagWrite(tp, tagWrites.get)(tpr)
            case OptionVal.None => FutureDone // no tags, skip
          }
        }
        .runWith(Sink.ignore)
    } else {
      log.debug(
        "[{}] Recovery is starting before the latest tag writes tag progress. Min progress [{}]. From sequence nr of recovery: [{}]",
        pid,
        minProgressNr,
        fromSequenceNr)
      FutureDone
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CassandraJournal {
  private[akka] type Tag = String
  private[akka] type PersistenceId = String
  private[akka] type SequenceNr = Long
  private[akka] type TagPidSequenceNr = Long

  private case object Init

  private case class WriteFinished(pid: String, f: Future[Done]) extends NoSerializationVerificationNeeded

  private case class DeleteFinished(pid: String, toSequenceNr: Long, f: Try[Unit])
      extends NoSerializationVerificationNeeded
  private case class PendingDelete(pid: String, toSequenceNr: Long, p: Promise[Unit])
      extends NoSerializationVerificationNeeded

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])

  private[akka] case class Serialized(
      persistenceId: String,
      sequenceNr: Long,
      serialized: ByteBuffer,
      tags: Set[String],
      eventAdapterManifest: String,
      serManifest: String,
      serId: Int,
      writerUuid: String,
      meta: Option[SerializedMeta],
      timeUuid: UUID,
      timeBucket: TimeBucket)

  private[akka] case class SerializedMeta(serialized: ByteBuffer, serManifest: String, serId: Int)

  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)
  private case class MessageId(persistenceId: String, sequenceNr: Long)

  class EventDeserializer(system: ActorSystem) {

    private val serialization = SerializationExtension(system)
    val columnDefinitionCache = new ColumnDefinitionCache

    def deserializeEvent(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[Any] =
      try {

        def meta: OptionVal[AnyRef] = {
          if (columnDefinitionCache.hasMetaColumns(row)) {
            row.getByteBuffer("meta") match {
              case null =>
                OptionVal.None // no meta data
              case metaBytes =>
                // has meta data, wrap in EventWithMetaData
                val metaSerId = row.getInt("meta_ser_id")
                val metaSerManifest = row.getString("meta_ser_manifest")
                val meta = serialization.deserialize(Bytes.getArray(metaBytes), metaSerId, metaSerManifest) match {
                  case Success(m) => m
                  case Failure(_) =>
                    // don't fail replay/query because of deserialization problem with meta data
                    // see motivation in UnknownMetaData
                    UnknownMetaData(metaSerId, metaSerManifest)
                }
                OptionVal.Some(meta)
            }
          } else {
            // for backwards compatibility, when table was not altered, meta columns not added
            OptionVal.None // no meta data
          }
        }

        val bytes = Bytes.getArray(row.getByteBuffer("event"))
        val serId = row.getInt("ser_id")
        val manifest = row.getString("ser_manifest")

        serialization.serializerByIdentity.get(serId) match {
          case Some(asyncSerializer: AsyncSerializer) =>
            Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
              asyncSerializer.fromBinaryAsync(bytes, manifest).map { event =>
                meta match {
                  case OptionVal.None    => event
                  case OptionVal.Some(m) => EventWithMetaData(event, m)
                }
              }
            }

          case _ =>
            def deserializedEvent: AnyRef = {
              // Serialization.deserialize adds transport info
              val event = serialization.deserialize(bytes, serId, manifest).get
              meta match {
                case OptionVal.None    => event
                case OptionVal.Some(m) => EventWithMetaData(event, m)
              }
            }

            if (async) Future(deserializedEvent)
            else Future.successful(deserializedEvent)
        }

      } catch {
        case NonFatal(e) => Future.failed(e)
      }
  }
}
