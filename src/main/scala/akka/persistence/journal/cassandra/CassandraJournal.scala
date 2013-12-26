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

  private val messageMarker = "A"
  private val deletionMarker = "B"
  private def confirmMarker(channelId: String) = s"C-${channelId}"

  val serialization = SerializationExtension(context.system)
  val session = cluster.connect()

  import context.dispatcher
  import statements._

  session.execute(createKeyspace(config.getInt("replication-factor")))
  session.execute(createTable)

  val preparedWrite = session.prepare(insertMessage).setConsistencyLevel(ConsistencyLevel.valueOf(config.getString("write-consistency")))
  val preparedReplay = session.prepare(selectMessages).setConsistencyLevel(ConsistencyLevel.valueOf(config.getString("read-consistency")))

  def writeAsync(persistentBatch: Seq[PersistentRepr]): Future[Unit] = {
    val batch = new BatchStatement
    persistentBatch.foreach { p =>
      batch.add(preparedWrite.bind(p.processorId, p.sequenceNr: JLong, messageMarker, persistentToByteBuffer(p)))
    }
    session.executeAsync(batch).map(_ => ())
  }

  def deleteAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = ???
  def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = ???

  def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    val bytes = Array.ofDim[Byte](b.remaining())
    b.get(bytes)
    serialization.deserialize(bytes, classOf[PersistentRepr]).get
  }

  override def postStop(): Unit = {
    session.shutdown()
    cluster.shutdown()
  }
}
