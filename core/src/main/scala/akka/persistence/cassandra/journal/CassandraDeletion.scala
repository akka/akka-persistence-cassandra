/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.datastax.driver.core.policies.{ LoggingRetryPolicy, RetryPolicy }
import com.datastax.driver.core._
import java.lang.{ Long => JLong }

import scala.language.implicitConversions
import akka.persistence.cassandra._
import akka.Done
import akka.event.LoggingAdapter
import akka.persistence.cassandra.journal.CassandraDeletion.{ MessageId, PartitionInfo }

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }

object CassandraDeletion {
  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)
  private case class MessageId(persistenceId: String, sequenceNr: Long)
}

trait CassandraDeletion extends CassandraStatements {
  private[akka] val session: CassandraSession

  private[akka] val log: LoggingAdapter
  private[akka] def config: CassandraJournalConfig
  private[akka] def queries: CassandraReadJournal
  private[akka] implicit val ec: ExecutionContext
  private[akka] implicit val materializer: ActorMaterializer

  private[akka] val readRetryPolicy: RetryPolicy
  private lazy val deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.deleteRetries))

  def preparedSelectDeletedTo = session.prepare(selectDeletedTo)
    .map(_.setConsistencyLevel(config.readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedSelectHighestSequenceNr = session.prepare(selectHighestSequenceNr)
    .map(_.setConsistencyLevel(config.readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedInsertDeletedTo = session.prepare(insertDeletedTo)
    .map(_.setConsistencyLevel(config.writeConsistency).setIdempotent(true))
  def preparedDeleteMessages = session.prepare(deleteMessages).map(_.setIdempotent(true))

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap {
      highestDeletedSequenceNumber =>
        asyncReadLowestSequenceNr(persistenceId, 1L, highestDeletedSequenceNumber, Some(config.readConsistency), Some(readRetryPolicy)).flatMap {
          lowestSequenceNr =>
            asyncFindHighestSequenceNr(persistenceId, lowestSequenceNr, config.targetPartitionSize).flatMap {
              highestSequenceNr =>
                val lowestPartition = partitionNr(lowestSequenceNr, config.targetPartitionSize)
                val toSeqNr = math.min(toSequenceNr, highestSequenceNr)
                val highestPartition = partitionNr(toSeqNr, config.targetPartitionSize) + 1 // may have been moved to the next partition

                val boundInsertDeletedTo = preparedInsertDeletedTo.map(_.bind(persistenceId, toSeqNr: JLong))
                val logicalDelete = boundInsertDeletedTo.flatMap(session.executeWrite)

                def partitionInfos: immutable.Seq[Future[PartitionInfo]] = (lowestPartition to highestPartition).map(partitionInfo(persistenceId, _, toSeqNr))

                def physicalDelete(): Unit = {
                  if (config.cassandra2xCompat) {
                    def asyncDeleteMessages(partitionNr: Long, messageIds: Seq[MessageId]): Future[Unit] = {
                      val boundStatements = messageIds.map(mid =>
                        preparedDeleteMessages.map(_.bind(mid.persistenceId, partitionNr: JLong, mid.sequenceNr: JLong)))
                      Future.sequence(boundStatements).flatMap {
                        stmts =>
                          executeBatch(batch => stmts.foreach(batch.add), deleteRetryPolicy)
                      }
                    }

                    partitionInfos.map(future => future.flatMap(pi => {
                      Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(config.maxMessageBatchSize).map {
                        group =>
                          {
                            val delete = asyncDeleteMessages(pi.partitionNr, group map (MessageId(persistenceId, _)))
                            delete.failed.foreach {
                              e =>
                                log.warning(s"Unable to complete deletes for persistence id {}, toSequenceNr {}. " +
                                  "The plugin will continue to function correctly but you will need to manually delete the old messages. " +
                                  "Caused by: [{}: {}]", persistenceId, toSequenceNr, e.getClass.getName, e.getMessage)
                            }
                            delete
                          }
                      })
                    }))

                  } else {
                    Future.sequence(partitionInfos.map(future => future.flatMap {
                      pi =>
                        val boundDeleteMessages = preparedDeleteMessages.map(_.bind(persistenceId, pi.partitionNr: JLong, toSeqNr: JLong))
                        boundDeleteMessages.flatMap(execute(_, deleteRetryPolicy))
                    }))
                      .failed.foreach { e =>
                        log.warning("Unable to complete deletes for persistence id {}, toSequenceNr {}. " +
                          "The plugin will continue to function correctly but you will need to manually delete the old messages. " +
                          "Caused by: [{}: {}]", persistenceId, toSequenceNr, e.getClass.getName, e.getMessage)
                      }
                  }
                }

                logicalDelete.foreach(_ => physicalDelete())

                logicalDelete.map(_ => Done)
            }
        }
    }
  }

  private def execute(stmt: Statement, retryPolicy: RetryPolicy): Future[Unit] = {
    stmt.setConsistencyLevel(config.writeConsistency).setRetryPolicy(retryPolicy)
    session.executeWrite(stmt).map(_ => ())
  }

  private def partitionInfo(persistenceId: String, partitionNr: Long, maxSequenceNr: Long): Future[PartitionInfo] = {
    val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, partitionNr: JLong))
    boundSelectHighestSequenceNr.flatMap(session.selectOne)
      .map(row => row.map(s => PartitionInfo(partitionNr, minSequenceNr(partitionNr), math.min(s.getLong("sequence_nr"), maxSequenceNr)))
        .getOrElse(PartitionInfo(partitionNr, minSequenceNr(partitionNr), -1)))
  }

  private[akka] def asyncHighestDeletedSequenceNumber(persistenceId: String): Future[Long] = {
    val boundSelectDeletedTo = preparedSelectDeletedTo.map(_.bind(persistenceId))
    boundSelectDeletedTo.flatMap(session.selectOne)
      .map(rowOption => rowOption.map(_.getLong("deleted_to")).getOrElse(0))
  }

  // TODO create a custom extractor that doesn't deserialise the event
  private[akka] def asyncReadLowestSequenceNr(
    persistenceId:                String,
    fromSequenceNr:               Long,
    highestDeletedSequenceNumber: Long,
    readConsistency:              Option[ConsistencyLevel],
    retryPolicy:                  Option[RetryPolicy]
  ): Future[Long] = {
    queries
      .eventsByPersistenceId(
        persistenceId,
        fromSequenceNr,
        highestDeletedSequenceNumber,
        1,
        1,
        None,
        "asyncReadLowestSequenceNr",
        readConsistency,
        retryPolicy,
        extractor = Extractors.sequenceNumber
      )
      .map(_.sequenceNr)
      .runWith(Sink.headOption)
      .map {
        case Some(sequenceNr) => sequenceNr
        case None             => fromSequenceNr
      }
  }

  private[akka] def asyncFindHighestSequenceNr(persistenceId: String, fromSequenceNr: Long, partitionSize: Long): Future[Long] = {
    def find(currentPnr: Long, currentSnr: Long): Future[Long] = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, currentPnr: JLong))
      boundSelectHighestSequenceNr.flatMap(session.selectOne)
        .map { rowOption =>
          rowOption.map { row =>
            (row.getBool("used"), row.getLong("sequence_nr"))
          }
        }
        .flatMap {
          // never been to this partition
          case None                   => Future.successful(currentSnr)
          // don't currently explicitly set false
          case Some((false, _))       => Future.successful(currentSnr)
          // everything deleted in this partition, move to the next
          case Some((true, 0))        => find(currentPnr + 1, currentSnr)
          case Some((_, nextHighest)) => find(currentPnr + 1, nextHighest)
        }
    }

    find(partitionNr(fromSequenceNr, partitionSize), fromSequenceNr)
  }

  private def executeBatch(body: BatchStatement ⇒ Unit, retryPolicy: RetryPolicy): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(config.writeConsistency).setRetryPolicy(retryPolicy).asInstanceOf[BatchStatement]
    body(batch)
    session.underlying().flatMap(_.executeAsync(batch)).map(_ => ())
  }

  private def minSequenceNr(partitionNr: Long): Long =
    partitionNr * config.targetPartitionSize + 1

}
