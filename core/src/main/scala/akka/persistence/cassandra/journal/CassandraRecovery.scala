/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import scala.concurrent._
import java.lang.{ Long => JLong }
import akka.actor.ActorLogging
import akka.persistence.PersistentRepr
import akka.stream.ActorMaterializer
import akka.persistence.cassandra.listenableFutureToFuture

trait CassandraRecovery extends ActorLogging {
  this: CassandraJournal =>
  import config._
  import context.dispatcher

  override def asyncReplayMessages(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long,
    max:            Long
  )(replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    queries
      .eventsByPersistenceId(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        max,
        replayMaxResultSize,
        None,
        "asyncReplayMessages",
        someReadConsistency,
        someReadRetryPolicy
      )
      .runForeach(replayCallback)
      .map(_ => ())

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap { h =>
      asyncFindHighestSequenceNr(persistenceId, math.max(fromSequenceNr, h))
    }

  private def asyncFindHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {

    def find(currentPnr: Long, currentSnr: Long): Future[Long] = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, currentPnr: JLong))
      boundSelectHighestSequenceNr.flatMap(session.selectResultSet)
        .map { rs =>
          Option(rs.one()).map { row =>
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

    find(partitionNr(fromSequenceNr), fromSequenceNr)
  }

  def asyncHighestDeletedSequenceNumber(persistenceId: String): Future[Long] = {
    val boundSelectDeletedTo = preparedSelectDeletedTo.map(_.bind(persistenceId))
    boundSelectDeletedTo.flatMap(session.selectResultSet)
      .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))
  }
}
