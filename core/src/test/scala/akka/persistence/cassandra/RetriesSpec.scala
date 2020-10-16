/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.ConstantFun
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

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
      @volatile var called = 0
      Retries
        .retry(() => {
          called += 1
          Future.failed(new RuntimeException("cats"))
        }, 3, ConstantFun.scalaAnyThreeToUnit, 1.milli, 2.millis, 0.1)
        .failed
        .futureValue
      called shouldEqual 3
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
