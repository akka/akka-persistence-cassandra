/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import scala.concurrent.duration.DurationInt

import org.scalatest.{ MustMatchers, WordSpecLike }

import akka.actor.{ ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }

class PubSubThrottlerSpec extends TestKit(ActorSystem("CassandraConfigCheckerSpec"))
  with WordSpecLike with MustMatchers {
  "PubSubThrottler" should {
    "eat up duplicate messages that arrive within the same [interval] window" in {
      val delegate = TestProbe()
      val throttler = system.actorOf(Props(new PubSubThrottler(delegate.ref, 5.seconds)))

      throttler ! "hello"
      throttler ! "hello"
      throttler ! "hello"
      delegate.within(2.seconds) {
        delegate.expectMsg("hello")
      }
      // Only first "hello" makes it through during the first interval.
      delegate.expectNoMessage(2.seconds)

      // Eventually, the interval will roll over and forward ONE further hello.
      delegate.expectMsg(10.seconds, "hello")
      delegate.expectNoMessage(2.seconds)

      throttler ! "hello"
      delegate.within(2.seconds) {
        delegate.expectMsg("hello")
      }
    }

    "allow differing messages to pass through within the same [interval] window" in {
      val delegate = TestProbe()
      val throttler = system.actorOf(Props(new PubSubThrottler(delegate.ref, 5.seconds)))
      throttler ! "hello"
      throttler ! "world"
      delegate.within(2.seconds) {
        delegate.expectMsg("hello")
        delegate.expectMsg("world")
      }
    }
  }
}
