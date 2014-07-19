package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent._

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence._
import akka.persistence.cassandra._
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy

class CassandraJournal extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {
  val config = context.system.settings.config.getConfig("cassandra-journal")
  val extension = Persistence(context.system)

  val keyspace = config.getString("keyspace")
  val table = config.getString("table")

  val maxPartitionSize = config.getInt("max-partition-size") // TODO: make persistent
  val maxResultSize = config.getInt("max-result-size")

  val serialization = SerializationExtension(context.system)

  private val clusterBuilder = Cluster.builder.addContactPoints(config.getStringList("contact-points").asScala: _*)

  if(config.hasPath("port")) {
    clusterBuilder.withPort(config.getInt("port"))
  }

  if(config.hasPath("authentication")) {
    clusterBuilder.withCredentials(
      config.getString("authentication.username"),
      config.getString("authentication.password"))
  }

  if(config.hasPath("local-datacenter")) {
    clusterBuilder.withLoadBalancingPolicy(
      new DCAwareRoundRobinPolicy(config.getString("local-datacenter"))
    )
  }

  val cluster = clusterBuilder.build
  val session = cluster.connect()

  session.execute(createKeyspace(config.getInt("replication-factor")))
  session.execute(createTable)

  val writeConsistency = ConsistencyLevel.valueOf(config.getString("write-consistency"))
  val readConsistency = ConsistencyLevel.valueOf(config.getString("read-consistency"))

  val preparedWriteHeader = session.prepare(writeHeader)
  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedConfirmMessage = session.prepare(confirmMessage)
  val preparedDeleteLogical = session.prepare(deleteMessageLogical)
  val preparedDeletePermanent = session.prepare(deleteMessagePermanent)
  val preparedSelectHeader = session.prepare(selectHeader).setConsistencyLevel(readConsistency)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)

  def asyncWriteMessages(messages: Seq[PersistentRepr]): Future[Unit] = executeBatch { batch =>
    messages.foreach { m =>
      val pnr = partitionNr(m.sequenceNr)
      if (partitionNew(m.sequenceNr)) batch.add(preparedWriteHeader.bind(m.processorId, pnr: JLong))
      batch.add(preparedWriteMessage.bind(m.processorId, pnr: JLong, m.sequenceNr: JLong, persistentToByteBuffer(m)))
    }
  }

  def asyncWriteConfirmations(confirmations: Seq[PersistentConfirmation]): Future[Unit] = executeBatch { batch =>
    confirmations.foreach { c =>
      batch.add((preparedConfirmMessage.bind(c.processorId, partitionNr(c.sequenceNr): JLong, c.sequenceNr: JLong, confirmMarker(c.channelId))))
    }
  }

  def asyncDeleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Future[Unit] = executeBatch { batch =>
    messageIds.foreach { mid =>
      val stmt =
        if (permanent) preparedDeletePermanent.bind(mid.processorId, partitionNr(mid.sequenceNr): JLong, mid.sequenceNr: JLong)
        else preparedDeleteLogical.bind(mid.processorId, partitionNr(mid.sequenceNr): JLong, mid.sequenceNr: JLong)
      batch.add(stmt)
    }
  }

  def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    val fromSequenceNr = readLowestSequenceNr(processorId, 1L)
    val asyncDeletions = (fromSequenceNr to toSequenceNr).grouped(extension.settings.journal.maxDeletionBatchSize).map { group =>
      asyncDeleteMessages(group map (PersistentIdImpl(processorId, _)), permanent)
    }
    Future.sequence(asyncDeletions).map(_ => ())
  }

  def executeBatch(body: BatchStatement ⇒ Unit): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
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
    session.close()
    cluster.close()
  }
}
