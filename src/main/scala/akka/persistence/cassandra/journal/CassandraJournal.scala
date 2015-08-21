package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.concurrent._
import scala.collection.immutable.Seq
import scala.collection.JavaConversions._
import scala.math.min
import scala.util.{Success, Failure, Try}

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence._
import akka.persistence.cassandra._
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes

class CassandraJournal extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {

  val config = new CassandraJournalConfig(context.system.settings.config.getConfig("cassandra-journal"))
  val serialization = SerializationExtension(context.system)

  import config._

  val cluster = clusterBuilder.build
  val session = cluster.connect()

  case class MessageId(persistenceId: String, sequenceNr: Long)

  if (config.keyspaceAutoCreate) {
    retry(config.keyspaceAutoCreateRetries) {
      session.execute(createKeyspace)
    }
  }
  session.execute(createTable)
  session.execute(createConfigTable)

  val persistentConfig: Map[String, String] = session.execute(selectConfig).all().toList
    .map(row => (row.getString("property"), row.getString("value"))).toMap

  persistentConfig.get(CassandraJournalConfig.TargetPartitionProperty).foreach(oldValue =>
    require(oldValue.toInt == config.targetPartitionSize, "Can't change target-partition-size"))

  session.execute(writeConfig, CassandraJournalConfig.TargetPartitionProperty, config.targetPartitionSize.toString)

  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedDeletePermanent = session.prepare(deleteMessage)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  val preparedCheckInUse = session.prepare(selectInUse).setConsistencyLevel(readConsistency)
  val preparedWriteInUse = session.prepare(writeInUse)
  val preparedSelectHighestSequenceNr = session.prepare(selectHighestSequenceNr).setConsistencyLevel(readConsistency)

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    val serialized = messages.map(aw => Try { SerializedAtomicWrite(
        aw.payload.head.persistenceId,
        aw.payload.map(pr => Serialized(pr.sequenceNr, persistentToByteBuffer(pr))))
    })
    val result = serialized.map(a => a.map(_ => ()))

    val byPersistenceId = serialized.collect({ case Success(caw) => caw }).groupBy(_.persistenceId).values
    val boundStatements = byPersistenceId.map(statementGroup)

    val batchStatements = boundStatements.map({ unit =>
        executeBatch(batch => unit.foreach(batch.add))
    })
    val promise = Promise[Seq[Try[Unit]]]()

    Future.sequence(batchStatements).onComplete {
      case Success(_) => promise.complete(Success(result))
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  private def statementGroup(atomicWrites: Seq[SerializedAtomicWrite]): Seq[BoundStatement] = {
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
    val highestPartition = partitionNr(toSequenceNr) + 1 // may have been moved to the next partition
    val partitionInfos = (lowestPartition to highestPartition).map(partitionInfo(persistenceId, _, toSequenceNr))

    val asyncDeletions = partitionInfos.map( future => future.flatMap( pi => {
      Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(config.maxMessageBatchSize).map { group => {
        asyncDeleteMessages(pi.partitionNr, group map (MessageId(persistenceId, _)))
      }
      })
    }))
    Future.sequence(asyncDeletions).map(_ => ())
  }

  private def partitionInfo(persistenceId: String, partitionNr: Long, maxSequenceNr: Long): Future[PartitionInfo] = {
    session.executeAsync(preparedSelectHighestSequenceNr.bind(persistenceId, partitionNr: JLong))
      .map(rs => Option(rs.one()))
      .map(row => row.map(s => PartitionInfo(partitionNr, minSequenceNr(partitionNr), min(s.getLong("sequence_nr"), maxSequenceNr)))
        .getOrElse(PartitionInfo(partitionNr, minSequenceNr(partitionNr), -1)))
  }

  private def asyncDeleteMessages(partitionNr: Long, messageIds: Seq[MessageId]): Future[Unit] = executeBatch { batch =>
    messageIds.foreach { mid =>
      batch.add(preparedDeletePermanent.bind(mid.persistenceId, partitionNr: JLong, mid.sequenceNr: JLong))
    }
  }

  private def executeBatch(body: BatchStatement ⇒ Unit): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
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

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(sequenceNr: Long, serialized: ByteBuffer)
  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)
}
