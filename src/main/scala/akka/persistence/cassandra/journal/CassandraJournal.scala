package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.collection.immutable.Seq
import scala.concurrent._

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence._
import akka.persistence.cassandra._
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes
import akka.actor.ActorLogging

class CassandraJournal extends AsyncWriteJournal with CassandraRecovery with CassandraStatements with CassandraPlugin with ActorLogging{
  val config = new CassandraJournalConfig(context.system.settings.config.getConfig("cassandra-journal"))
  val serialization = SerializationExtension(context.system)
  val persistence = Persistence(context.system)
  val logger = log

  private def tableName = s"${config.keyspace}.${config.table}"

  import config._

  val cluster = clusterBuilder.build
  val session = cluster.connect()
  
  createKeyspace(session)
  createJournalTable(session)

  val preparedSelectHeader = session.prepare(selectHeader).setConsistencyLevel(readConsistency)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)

  def asyncWriteMessages(messages: Seq[PersistentRepr]): Future[Unit] = {
    val preparedWriteBatch = new StringBuilder
    preparedWriteBatch.append("BEGIN BATCH")
    messages.foreach { m => 
      val pnr : JLong = partitionNr(m.sequenceNr)
      val processorId = m.processorId
      val sequenceNr : JLong = m.sequenceNr
      val byteBuffer = Bytes.toHexString(persistentToByteBuffer(m))
      if (partitionNew(m.sequenceNr)){
        var psHeader = s"INSERT INTO ${tableName} (processor_id, partition_nr, sequence_nr, marker, message) VALUES ('${processorId}', ${pnr}, 0, 'H', 0x00)"
        preparedWriteBatch.append("\n")
        preparedWriteBatch.append(psHeader)
      }
      var psMessage = s"INSERT INTO ${tableName} (processor_id, partition_nr, sequence_nr, marker, message) VALUES ('${processorId}', ${pnr}, ${sequenceNr}, 'A', ${byteBuffer})"
      preparedWriteBatch.append("\n")
      preparedWriteBatch.append(psMessage)
      }
     preparedWriteBatch.append("\n")
     preparedWriteBatch.append("APPLY BATCH;")
     executeBatch(preparedWriteBatch.toString)
  }

  def asyncWriteConfirmations(confirmations: Seq[PersistentConfirmation]): Future[Unit] = {
    val preparedConfirmBatch : StringBuilder = new StringBuilder
    preparedConfirmBatch.append("BEGIN BATCH")
    confirmations.foreach { c =>
      val processorId = c.processorId
      val partitionNR :JLong = partitionNr(c.sequenceNr)
      val sequenceNr :JLong = c.sequenceNr
      val confirmMark = confirmMarker(c.channelId)
      var psConfirmation = s"INSERT INTO ${tableName} (processor_id, partition_nr, sequence_nr, marker, message)VALUES ('${processorId}', ${partitionNR}, ${sequenceNr}, '${confirmMark}', 0x00)"
      preparedConfirmBatch.append("\n")
      preparedConfirmBatch.append(psConfirmation)
    }
    preparedConfirmBatch.append("\n")
    preparedConfirmBatch.append("APPLY BATCH;")
    executeBatch(preparedConfirmBatch.toString)
  }

  def asyncDeleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Future[Unit] = {
    val preparedDeletePermanentBatch :StringBuilder = new StringBuilder
    preparedDeletePermanentBatch.append("BEGIN BATCH")
    val preparedDeleteLogicalBatch : StringBuilder = new StringBuilder
    preparedDeleteLogicalBatch.append("BEGIN BATCH")
    messageIds.foreach { mid =>
        val processorId = mid.processorId
        val partitionNR :JLong = partitionNr(mid.sequenceNr)
        val sequenceNr :JLong = mid.sequenceNr
        if (permanent){
          var psDelPermanent = s"DELETE FROM ${tableName} WHERE processor_id = '${processorId}' AND partition_nr = ${partitionNR} AND sequence_nr = ${sequenceNr}"
          preparedDeletePermanentBatch.append("\n")
          preparedDeletePermanentBatch.append(psDelPermanent)
        }
        else { 
            var psDelLogical = s"INSERT INTO ${tableName} (processor_id, partition_nr, sequence_nr, marker, message) VALUES ('${processorId}', ${partitionNR}, ${sequenceNr}, 'B',0x00)"
            preparedDeleteLogicalBatch.append("\n")
            preparedDeleteLogicalBatch.append(psDelLogical)
           }
        }
        if(permanent){
          preparedDeletePermanentBatch.append("\n")
          preparedDeletePermanentBatch.append("APPLY BATCH;")
          executeBatch(preparedDeletePermanentBatch.toString)
        }
        else {
          preparedDeleteLogicalBatch.append("\n")
          preparedDeleteLogicalBatch.append("APPLY BATCH;")
          executeBatch(preparedDeleteLogicalBatch.toString)
        } 
  }

  def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    val fromSequenceNr = readLowestSequenceNr(processorId, 1L)
    val asyncDeletions = (fromSequenceNr to toSequenceNr).grouped(persistence.settings.journal.maxDeletionBatchSize).map { group =>
      asyncDeleteMessages(group map (PersistentIdImpl(processorId, _)), permanent)
    }
    Future.sequence(asyncDeletions).map(_ => ())
  }

  def executeBatch(batch: String): Future[Unit] = {
    val stmt = new SimpleStatement(batch).setConsistencyLevel(writeConsistency).asInstanceOf[SimpleStatement]
    session.executeAsync(stmt).map(_ => ())
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