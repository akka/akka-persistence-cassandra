package akka.persistence.journal

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

import akka.actor._
import akka.persistence._
import akka.persistence.JournalProtocol._
import akka.testkit._

import org.scalatest._

trait JournalSpec extends WordSpecLike with Matchers with BeforeAndAfterEach { this: TestKit =>
  val extension = Persistence(system)
  val journal: ActorRef = extension.journalFor(null)
  val counter = new AtomicInteger(0)

  var pid: String = _

  var senderProbe: TestProbe = _
  var receiverProbe: TestProbe = _

  override def beforeEach(): Unit = {
    senderProbe = TestProbe()
    receiverProbe = TestProbe()

    pid = s"p-${counter.incrementAndGet()}"
    writeMessages(1, 5, pid, senderProbe.ref)
  }

  def writeMessages(from: Int, to: Int, pid: String, sender: ActorRef): Unit = {
    val msgs = from to to map { i => PersistentRepr(payload = s"a-${i}", sequenceNr = i, processorId = pid, sender = sender) }
    val probe = TestProbe()

    journal ! WriteMessages(msgs, probe.ref)

    probe.expectMsg(WriteMessagesSuccess)
    from to to foreach { i =>
      probe.expectMsgPF() { case WriteMessageSuccess(PersistentImpl(payload, `i`, `pid`, _, _, `sender`)) => payload should be (s"a-${i}") }
    }
  }

  def replayedMessage(snr: Long, deleted: Boolean = false, confirms: Seq[String] = Nil): ReplayedMessage =
    ReplayedMessage(PersistentImpl(s"a-${snr}", snr, pid, deleted, confirms, senderProbe.ref))

  def subscribe[T : ClassTag](subscriber: ActorRef) =
    system.eventStream.subscribe(subscriber, implicitly[ClassTag[T]].runtimeClass)

  "A journal" must {
    "replay all messages" in {
      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      1 to 5 foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay messages using a lower sequence number bound" in {
      journal ! ReplayMessages(3, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      3 to 5 foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay messages using an upper sequence number bound" in {
      journal ! ReplayMessages(1, 3, Long.MaxValue, pid, receiverProbe.ref)
      1 to 3 foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay messages using a count limit" in {
      journal ! ReplayMessages(1, Long.MaxValue, 3, pid, receiverProbe.ref)
      1 to 3 foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay messages using a lower and upper sequence number bound" in {
      journal ! ReplayMessages(2, 4, Long.MaxValue, pid, receiverProbe.ref)
      2 to 4 foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay messages using a lower and upper sequence number bound and a count limit" in {
      journal ! ReplayMessages(2, 4, 2, pid, receiverProbe.ref)
      2 to 3 foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay a single if lower equals upper sequence number bound" in {
      journal ! ReplayMessages(2, 2, Long.MaxValue, pid, receiverProbe.ref)
      2 to 2 foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay a single message if count limit equals 1" in {
      journal ! ReplayMessages(2, 4, 1, pid, receiverProbe.ref)
      2 to 2 foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "not replay messages if count limit equals 0" in {
      journal ! ReplayMessages(2, 4, 0, pid, receiverProbe.ref)
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "not replay messages if lower is greater than upper sequence number bound" in {
      journal ! ReplayMessages(3, 2, Long.MaxValue, pid, receiverProbe.ref)
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "not replay permanently deleted messages (individual deletion)" in {
      val msgIds = List(PersistentIdImpl(pid, 3), PersistentIdImpl(pid, 4))
      journal ! DeleteMessages(msgIds, true, Some(receiverProbe.ref))
      receiverProbe.expectMsg(DeleteMessagesSuccess(msgIds))

      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      List(1, 2, 5) foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "not replay permanently deleted messages (range deletion)" in {
      val cmd = DeleteMessagesTo(pid, 3, true)
      val sub = TestProbe()

      journal ! cmd
      subscribe[DeleteMessagesTo](sub.ref)
      sub.expectMsg(cmd)

      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      List(4, 5) foreach { i => receiverProbe.expectMsg(replayedMessage(i)) }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay logically deleted messages with deleted=true (individual deletion)" in {
      val msgIds = List(PersistentIdImpl(pid, 3), PersistentIdImpl(pid, 4))
      journal ! DeleteMessages(msgIds, false, Some(receiverProbe.ref))
      receiverProbe.expectMsg(DeleteMessagesSuccess(msgIds))

      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref, replayDeleted = true)
      1 to 5 foreach { i =>
        i match {
          case 1 | 2 | 5 => receiverProbe.expectMsg(replayedMessage(i))
          case 3 | 4     => receiverProbe.expectMsg(replayedMessage(i, deleted = true))
        }
      }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
    "replay logically deleted messages with deleted=true (range deletion)" in {
      val cmd = DeleteMessagesTo(pid, 3, false)
      val sub = TestProbe()

      journal ! cmd
      subscribe[DeleteMessagesTo](sub.ref)
      sub.expectMsg(cmd)

      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref, replayDeleted = true)
      1 to 5 foreach { i =>
        i match {
          case 1 | 2 | 3 => receiverProbe.expectMsg(replayedMessage(i, deleted = true))
          case 4 | 5     => receiverProbe.expectMsg(replayedMessage(i))
        }
      }
      receiverProbe.expectMsg(ReplayMessagesSuccess)
    }
  }
}
