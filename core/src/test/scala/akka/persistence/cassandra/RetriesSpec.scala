/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.TestProbe
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class RetriesSpec
    extends TestKit(ActorSystem("RetriesSpec"))
    with AnyWordSpecLike
    with ScalaFutures
    with BeforeAndAfterAll
    with Matchers {
  implicit val scheduler = system.scheduler
  implicit val ec = system.dispatcher
  "Retries" should {
    "retry N number of times" in {
      val failProbe = TestProbe()
      @volatile var called = 0
      val result = Retries
        .retry(() => {
          called += 1
          Future.failed(new RuntimeException(s"cats $called"))
        }, 3, (_, exc, _) => failProbe.ref ! exc, 1.milli, 2.millis, 0.1)
        .failed
        .futureValue
      called shouldEqual 3
      result.getMessage shouldEqual "cats 3"
      failProbe.expectMsgType[RuntimeException].getMessage shouldEqual "cats 1"
      failProbe.expectMsgType[RuntimeException].getMessage shouldEqual "cats 2"
      failProbe.expectNoMessage()
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
