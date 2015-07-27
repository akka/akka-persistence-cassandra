package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.concurrent._
import scala.collection.immutable.Seq
import scala.collection.JavaConversions._
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
  val preparedConfirmMessage = session.prepare(confirmMessage)
  val preparedDeletePermanent = session.prepare(deleteMessage)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  val preparedCheckInUse = session.prepare(selectInUse).setConsistencyLevel(readConsistency)
  val preparedWriteInUse = session.prepare(writeInUse)

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val groupedStatements = messages.map(statementGroup)
    val batchStatements = groupedStatements.map({
      case Success(atomicWrite) =>
        executeBatch(batch => atomicWrite.foreach(batch.add)).map(_ => Success(()))
      case Failure(e) =>
        Future.successful(Failure[Unit](e))
    })

    Future.sequence(batchStatements)
  }

  private def statementGroup(atomicWrite: AtomicWrite): Try[Seq[BoundStatement]] = Try {
    // hoping to remove this in 2.4-M3 https://github.com/akka/akka/issues/18076
    val maxPnr = partitionNr(atomicWrite.payload.last.sequenceNr)
    val firstSeq: JLong = atomicWrite.payload.head.sequenceNr
    val minPnr: JLong = partitionNr(firstSeq)
    val persistenceId: String = atomicWrite.payload.head.persistenceId

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    if (maxPnr - minPnr > 1) throw new RuntimeException("Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = atomicWrite.payload.map { m =>
      preparedWriteMessage.bind(m.persistenceId, maxPnr: JLong, m.sequenceNr: JLong, persistentToByteBuffer(m))
    }
    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstSeq) && minPnr != maxPnr) writes :+ preparedWriteInUse.bind(persistenceId, minPnr)
    else writes

  }

  private def asyncDeleteMessages(messageIds: Seq[MessageId]): Future[Unit] = executeBatch { batch =>
    messageIds.foreach { mid =>
      val firstPnr: JLong = partitionNr(mid.sequenceNr)
      val stmt = preparedDeletePermanent.bind(mid.persistenceId, firstPnr: JLong, mid.sequenceNr: JLong)
      // the message could be in next partition as a result of an AtomicWrite, alternative is a read before write
      val stmt2 = preparedDeletePermanent.bind(mid.persistenceId, firstPnr+1: JLong, mid.sequenceNr: JLong)
      batch.add(stmt)
      batch.add(stmt2)
    }
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val fromSequenceNr = readLowestSequenceNr(persistenceId, 1L)
    val asyncDeletions = (fromSequenceNr to toSequenceNr).grouped(config.targetPartitionSize / 2).map { group =>
      asyncDeleteMessages(group map (MessageId(persistenceId, _)))
    }
    Future.sequence(asyncDeletions).map(_ => ())
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

  private def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }
}
