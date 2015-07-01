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

  persistentConfig.get(CassandraJournalConfig.MaxPartitionProperty).foreach(oldValue =>
    require(oldValue.toInt == config.maxPartitionSize, "Can't change max-partition-size"))

  session.execute(writeConfig, CassandraJournalConfig.MaxPartitionProperty, config.maxPartitionSize.toString)

  val preparedWriteHeader = session.prepare(writeHeader)
  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedConfirmMessage = session.prepare(confirmMessage)
  val preparedDeletePermanent = session.prepare(deleteMessage)
  val preparedSelectHeader = session.prepare(selectHeader).setConsistencyLevel(readConsistency)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)

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
    atomicWrite.payload.flatMap { m =>
      val pnr = partitionNr(m.sequenceNr)
      val write = preparedWriteMessage.bind(m.persistenceId, pnr: JLong, m.sequenceNr: JLong, persistentToByteBuffer(m))
      if (partitionNew(m.sequenceNr)) Seq(preparedWriteHeader.bind(m.persistenceId, pnr: JLong), write) else Seq(write)
    }
  }

  private def asyncDeleteMessages(messageIds: Seq[MessageId]): Future[Unit] = executeBatch { batch =>
    messageIds.foreach { mid =>
      val stmt =
        preparedDeletePermanent.bind(mid.persistenceId, partitionNr(mid.sequenceNr): JLong, mid.sequenceNr: JLong)
      batch.add(stmt)
    }
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val fromSequenceNr = readLowestSequenceNr(persistenceId, 1L)
    val asyncDeletions = (fromSequenceNr to toSequenceNr).grouped(persistence.settings.journal.maxDeletionBatchSize).map { group =>
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
    (sequenceNr - 1L) / maxPartitionSize

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % maxPartitionSize == 0L

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
