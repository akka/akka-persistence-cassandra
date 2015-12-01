package akka.persistence.cassandra.query

import java.net.InetSocketAddress

import org.scalatest.{ MustMatchers, WordSpec }
import akka.persistence.cassandra.query.SequenceNumbers._

class SequenceNumbersSpec extends WordSpec with MustMatchers {

  "SequenceNumbers" must {
    "answer isNext" in {
      val seqNumbers = SequenceNumbers.empty
      seqNumbers.isNext("p1", 1L) must be(Yes)
      seqNumbers.isNext("p1", 2L) must be(PossiblyFirst)

      val seqNumbers2 = seqNumbers.updated("p1", 1L)
      seqNumbers2.isNext("p1", 2L) must be(Yes)
      seqNumbers2.isNext("p1", 1L) must be(Before)
      seqNumbers2.isNext("p1", 3L) must be(After)
      seqNumbers2.isNext("p1", 1000L) must be(After)
      seqNumbers2.isNext("p2", 1L) must be(Yes)
    }

    "keep current" in {
      val seqNumbers = SequenceNumbers.empty.updated("p1", 1L).updated("p2", 10L)
      seqNumbers.get("p1") must be(1L)
      seqNumbers.get("p2") must be(10L)
      seqNumbers.get("p3") must be(0L)
    }

    "handle updates around Int.MaxValue" in {
      val n = (Int.MaxValue - 1).toLong
      val seqNumbers = SequenceNumbers.empty.updated("p1", n)
      seqNumbers.isNext("p1", n + 1) must be(Yes)
      seqNumbers.isNext("p1", n) must be(Before)
      seqNumbers.isNext("p1", n + 10) must be(After)

      val seqNumbers2 = seqNumbers.updated("p1", n + 1)
      seqNumbers2.isNext("p1", n + 2) must be(Yes)
      seqNumbers2.isNext("p1", n) must be(Before)
      seqNumbers2.isNext("p1", n + 1) must be(Before)
      seqNumbers2.isNext("p1", n + 10) must be(After)

      val seqNumbers3 = seqNumbers.updated("p1", n + 2)
      seqNumbers3.isNext("p1", n + 3) must be(Yes)
      seqNumbers3.isNext("p1", n) must be(Before)
      seqNumbers3.isNext("p1", n + 1) must be(Before)
      seqNumbers3.isNext("p1", n + 2) must be(Before)
      seqNumbers3.isNext("p1", n + 10) must be(After)
    }
  }
}
