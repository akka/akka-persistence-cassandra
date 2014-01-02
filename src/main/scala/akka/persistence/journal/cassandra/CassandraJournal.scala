package akka.persistence.journal.cassandra

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent._

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes

class CassandraJournal extends AsyncWriteJournal with CassandraReplay with CassandraStatements {
  val config = context.system.settings.config.getConfig("cassandra-journal")

  val keyspace = config.getString("keyspace")
  val table = config.getString("table")

  val maxPartitionSize = config.getInt("max-partition-size") // TODO: make persistent
  val maxResultSize = config.getInt("max-result-size")

  val serialization = SerializationExtension(context.system)

  val cluster = Cluster.builder.addContactPoints(config.getStringList("contact-points").asScala: _*).build
  val session = cluster.connect()

  session.execute(createKeyspace(config.getInt("replication-factor")))
  session.execute(createTable)

  val writeConsistency = ConsistencyLevel.valueOf(config.getString("write-consistency"))
  val readConsistency = ConsistencyLevel.valueOf(config.getString("read-consistency"))

  val preparedWriteHeader = session.prepare(writeHeader).setConsistencyLevel(writeConsistency)
  val preparedWriteMessage = session.prepare(writeMessage).setConsistencyLevel(writeConsistency)
  val preparedConfirmMessage = session.prepare(confirmMessage).setConsistencyLevel(writeConsistency)
  val preparedDeleteLogical = session.prepare(deleteMessageLogical).setConsistencyLevel(writeConsistency)
  val preparedDeletePermanent = session.prepare(deleteMessagePermanent).setConsistencyLevel(writeConsistency)
  val preparedSelectHeader = session.prepare(selectHeader).setConsistencyLevel(readConsistency)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)

  def writeAsync(persistentBatch: Seq[PersistentRepr]): Future[Unit] = {
    val batch = new BatchStatement
    persistentBatch.foreach { p =>
      val pnr = partitionNr(p.sequenceNr)
      if (partitionNew(p.sequenceNr)) batch.add(preparedWriteHeader.bind(p.processorId, pnr: JLong))
      batch.add(preparedWriteMessage.bind(p.processorId, pnr: JLong, p.sequenceNr: JLong, persistentToByteBuffer(p)))
    }
    session.executeAsync(batch).map(_ => ())
  }

  def deleteAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    val batch = new BatchStatement

    fromSequenceNr to toSequenceNr foreach { sequenceNr =>
      val stmt =
        if (permanent) preparedDeletePermanent.bind(processorId, partitionNr(sequenceNr): JLong, sequenceNr: JLong)
        else preparedDeleteLogical.bind(processorId, partitionNr(sequenceNr): JLong, sequenceNr: JLong)
      batch.add(stmt)
    }
    session.executeAsync(batch).map(_ => ())
  }

  def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
    session.executeAsync(preparedConfirmMessage.bind(processorId, partitionNr(sequenceNr): JLong, sequenceNr: JLong, confirmMarker(channelId))).map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / maxPartitionSize

  def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % maxPartitionSize == 0L

  def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  private def confirmMarker(channelId: String) =
    s"C-${channelId}"

  override def postStop(): Unit = {
    session.shutdown()
    cluster.shutdown()
  }
}
