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

class CassandraJournal extends AsyncWriteJournal with CassandraReplay {
  val config = context.system.settings.config.getConfig("cassandra-journal")

  private val cluster = Cluster.builder.addContactPoints(config.getStringList("contact-points").asScala: _*).build
  private val statements = new CassandraStatements(config.getString("keyspace"), config.getString("table"))

  val serialization = SerializationExtension(context.system)
  val session = cluster.connect()

  import context.dispatcher
  import statements._

  session.execute(createKeyspace(config.getInt("replication-factor")))
  session.execute(createTable)

  private val writeConsistency = ConsistencyLevel.valueOf(config.getString("write-consistency"))
  private val readConsistency = ConsistencyLevel.valueOf(config.getString("read-consistency"))

  val preparedWrite = session.prepare(insertMessage).setConsistencyLevel(writeConsistency)
  val preparedReplay = session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  val preparedConfirm = session.prepare(confirmMessage).setConsistencyLevel(writeConsistency)
  val preparedDeleteLogical = session.prepare(deleteMessageLogical).setConsistencyLevel(writeConsistency)
  val preparedDeletePermanent = session.prepare(deleteMessagePermanent).setConsistencyLevel(writeConsistency)

  def writeAsync(persistentBatch: Seq[PersistentRepr]): Future[Unit] = {
    val batch = new BatchStatement
    persistentBatch.foreach(p => batch.add(preparedWrite.bind(p.processorId, p.sequenceNr: JLong, persistentToByteBuffer(p))))
    session.executeAsync(batch).map(_ => ())
  }

  def deleteAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    val batch = new BatchStatement
    fromSequenceNr to toSequenceNr foreach { sequenceNr =>
      val stmt =
        if (permanent) preparedDeletePermanent.bind(processorId, sequenceNr: JLong)
        else preparedDeleteLogical.bind(processorId, sequenceNr: JLong)
      batch.add(stmt)
    }
    session.executeAsync(batch).map(_ => ())
  }

  def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
    session.executeAsync(preparedConfirm.bind(processorId, sequenceNr: JLong, confirmMarker(channelId))).map(_ => ())
  }

  def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    val bytes = Array.ofDim[Byte](b.remaining())
    b.get(bytes)
    serialization.deserialize(bytes, classOf[PersistentRepr]).get
  }

  private def confirmMarker(channelId: String) =
    s"C-${channelId}"

  override def postStop(): Unit = {
    session.shutdown()
    cluster.shutdown()
  }
}
