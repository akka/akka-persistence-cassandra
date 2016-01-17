/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension
import com.datastax.driver.core._
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.policies.{ LoggingRetryPolicy, RetryPolicy }
import com.datastax.driver.core.utils.Bytes
import com.typesafe.config.Config
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.math.min
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

class CassandraJournal(cfg: Config) extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {

  val config = new CassandraJournalConfig(cfg)
  val serialization = SerializationExtension(context.system)

  import config._

  val writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.writeRetries))
  val deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.deleteRetries))

  case class MessageId(persistenceId: String, sequenceNr: Long)

  private[journal] class CassandraSession {

    val underlying: Session = connect()

    if (config.keyspaceAutoCreate) {
      retry(config.keyspaceAutoCreateRetries) {
        underlying.execute(createKeyspace)
      }
    }
    underlying.execute(createTable)
    underlying.execute(createMetatdataTable)
    underlying.execute(createConfigTable)

    val preparedWriteMessage = underlying.prepare(writeMessage)
    val preparedDeletePermanent = underlying.prepare(deleteMessage)
    val preparedSelectMessages = underlying.prepare(selectMessages).setConsistencyLevel(readConsistency)
    val preparedCheckInUse = underlying.prepare(selectInUse).setConsistencyLevel(readConsistency)
    val preparedWriteInUse = underlying.prepare(writeInUse)
    val preparedSelectHighestSequenceNr = underlying.prepare(selectHighestSequenceNr).setConsistencyLevel(readConsistency)
    val preparedSelectDeletedTo = underlying.prepare(selectDeletedTo).setConsistencyLevel(readConsistency)
    val preparedInsertDeletedTo = underlying.prepare(insertDeletedTo).setConsistencyLevel(writeConsistency)

    private def connect(): Session = {
      retry(config.connectionRetries + 1, config.connectionRetryDelay.toMillis)(clusterBuilder.build().connect())
    }

    def close(): Unit = {
      underlying.close()
      underlying.getCluster().close()
    }
  }

  private var sessionUsed = false

  private[journal] lazy val cassandraSession: CassandraSession = {
    val s = new CassandraSession

    new CassandraConfigChecker {
      override def session: Session = s.underlying
      override def config: CassandraJournalConfig = CassandraJournal.this.config
    }.initializePersistentConfig()

    sessionUsed = true
    s
  }

  def session: Session = cassandraSession.underlying

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! CassandraJournal.Init
  }

  override def receivePluginInternal: Receive = {
    case CassandraJournal.Init =>
      try {
        cassandraSession
      } catch {
        case NonFatal(e) =>
          log.warning(
            "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
            e.getMessage
          )
      }
  }

  override def postStop(): Unit = {
    if (sessionUsed)
      cassandraSession.close()
  }

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    // We must NOT catch serialization exceptions here because rejections will cause
    // holes in the sequence number series and we use the sequence numbers to detect
    // missing (delayed) events in the eventByTag query
    val serialized = messages.map(aw => SerializedAtomicWrite(
      aw.payload.head.persistenceId,
      aw.payload.map(pr => Serialized(pr.sequenceNr, persistentToByteBuffer(pr)))
    ))

    val byPersistenceId = serialized.groupBy(_.persistenceId).values
    val boundStatements = byPersistenceId.map(statementGroup)

    val batchStatements = boundStatements.map { unit =>
      unit.length match {
        case 0 => Future.successful(())
        case 1 => execute(unit.head, writeRetryPolicy)
        case _ => executeBatch(batch => unit.foreach(batch.add), writeRetryPolicy)
      }
    }
    val promise = Promise[Seq[Try[Unit]]]()

    Future.sequence(batchStatements).onComplete {
      case Success(_) => promise.complete(Success(Nil)) // Nil == all good
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  private def statementGroup(atomicWrites: Seq[SerializedAtomicWrite]): Seq[BoundStatement] = {
    import cassandraSession._
    val maxPnr = partitionNr(atomicWrites.last.payload.last.sequenceNr)
    val firstSeq = atomicWrites.head.payload.head.sequenceNr
    val minPnr = partitionNr(firstSeq)
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = all.map { m =>
      preparedWriteMessage.bind(persistenceId, maxPnr: JLong, m.sequenceNr: JLong, m.serialized)
    }
    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstSeq) && minPnr != maxPnr) writes :+ preparedWriteInUse.bind(persistenceId, minPnr: JLong)
    else writes

  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {

    val fromSequenceNr = readLowestSequenceNr(persistenceId, 1L)
    val lowestPartition = partitionNr(fromSequenceNr)
    val toSeqNr = math.min(toSequenceNr, readHighestSequenceNr(persistenceId, fromSequenceNr))
    val highestPartition = partitionNr(toSeqNr) + 1 // may have been moved to the next partition
    val partitionInfos = (lowestPartition to highestPartition).map(partitionInfo(persistenceId, _, toSeqNr))

    val logicalDelete = session.executeAsync(
      cassandraSession.preparedInsertDeletedTo.bind(persistenceId, toSeqNr: JLong)
    )

    partitionInfos.map(future => future.flatMap(pi => {
      Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(config.maxMessageBatchSize).map { group =>
        {
          val delete = asyncDeleteMessages(pi.partitionNr, group map (MessageId(persistenceId, _)))
          delete.onFailure {
            case e => log.warning(s"Unable to complete deletes for persistence id ${persistenceId}, toSequenceNr ${toSequenceNr}. The plugin will continue to function correctly but you will need to manually delete the old messages.", e)
          }
          delete
        }
      })
    }))

    logicalDelete.map(_ => ())
  }

  private def partitionInfo(persistenceId: String, partitionNr: Long, maxSequenceNr: Long): Future[PartitionInfo] = {
    session.executeAsync(
      cassandraSession.preparedSelectHighestSequenceNr.bind(persistenceId, partitionNr: JLong)
    )
      .map(rs => Option(rs.one()))
      .map(row => row.map(s => PartitionInfo(partitionNr, minSequenceNr(partitionNr), min(s.getLong("sequence_nr"), maxSequenceNr)))
        .getOrElse(PartitionInfo(partitionNr, minSequenceNr(partitionNr), -1)))
  }

  private def asyncDeleteMessages(partitionNr: Long, messageIds: Seq[MessageId]): Future[Unit] = executeBatch({ batch =>
    messageIds.foreach { mid =>
      batch.add(cassandraSession.preparedDeletePermanent.bind(mid.persistenceId, partitionNr: JLong, mid.sequenceNr: JLong))
    }
  }, deleteRetryPolicy)

  private def executeBatch(body: BatchStatement ⇒ Unit, retryPolicy: RetryPolicy): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).setRetryPolicy(retryPolicy).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  private def execute(stmt: Statement, retryPolicy: RetryPolicy): Future[Unit] = {
    stmt.setConsistencyLevel(writeConsistency).setRetryPolicy(retryPolicy)
    session.executeAsync(stmt).map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % targetPartitionSize == 0L

  private def minSequenceNr(partitionNr: Long): Long =
    partitionNr * targetPartitionSize + 1

  private def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(sequenceNr: Long, serialized: ByteBuffer)
  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)
}

private[journal] object CassandraJournal {
  private case object Init
}

class FixedRetryPolicy(number: Int) extends RetryPolicy {
  def onUnavailable(statement: Statement, cl: ConsistencyLevel, requiredReplica: Int, aliveReplica: Int, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  def onWriteTimeout(statement: Statement, cl: ConsistencyLevel, writeType: WriteType, requiredAcks: Int, receivedAcks: Int, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  def onReadTimeout(statement: Statement, cl: ConsistencyLevel, requiredResponses: Int, receivedResponses: Int, dataRetrieved: Boolean, nbRetry: Int): RetryDecision = retry(cl, nbRetry)

  private def retry(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < number) RetryDecision.retry(cl) else RetryDecision.rethrow()
  }
}
