/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import akka.stream.scaladsl.ImplicitMaterializer

import scala.concurrent._

import java.lang.{ Long => JLong }

import akka.actor.{ExtendedActorSystem, ActorLogging}
import akka.persistence.PersistentRepr

import akka.persistence.cassandra.listenableFutureToFuture
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal

trait CassandraRecovery extends ActorLogging with ImplicitMaterializer {
  this: CassandraJournal =>
  import config._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)
  private[this] val extendedActorSystem = context.system.asInstanceOf[ExtendedActorSystem]

  private[this] val queries: CassandraReadJournal =
    new CassandraReadJournal(
      extendedActorSystem,
      context.system.settings.config.getConfig("cassandra-query-journal"))

  override def asyncReplayMessages(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    queries
      .eventsByPersistenceId(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        max,
        replayMaxResultSize,
        None,
        "asyncReplayMessages")
      .runForeach(replayCallback)

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    asyncHighestDeletedSequenceNumber(persistenceId)
      .flatMap{ h =>
        asyncFindHighestSequenceNr(persistenceId, math.max(fromSequenceNr, h))
      }

  private def asyncFindHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    import cassandraSession._

    def find(currentPnr: Long, currentSnr: Long): Future[Long] = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      listenableFutureToFuture(
        session.executeAsync(preparedSelectHighestSequenceNr.bind(persistenceId, currentPnr: JLong)))
        .map { rs =>
          Option(rs.one()).map { row =>
            (row.getBool("used"), row.getLong("sequence_nr"))
          }
        }
        .flatMap {
          // never been to this partition
          case None => Future.successful(currentSnr)
          // don't currently explicitly set false
          case Some((false, _)) => Future.successful(currentSnr)
          // everything deleted in this partition, move to the next
          case Some((true, 0)) => find(currentPnr + 1, currentSnr)
          case Some((_, nextHighest)) => find(currentPnr + 1, nextHighest)
        }
      }

    find(partitionNr(fromSequenceNr), fromSequenceNr)
  }

  private[this] def asyncHighestDeletedSequenceNumber(partitionKey: String): Future[Long] = {
    listenableFutureToFuture(session.executeAsync(cassandraSession.preparedSelectDeletedTo.bind(partitionKey)))
      .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))
  }
}
