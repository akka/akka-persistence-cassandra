/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.actor.ActorSystem
import akka.cassandra.session.scaladsl.CassandraSessionRegistry
import akka.stream.{ ActorMaterializer, Materializer }
import akka.testkit.TestKit
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * All the tests must be run with a local Cassandra running on default port 9042.
 */
abstract class CassandraSpecBase(_system: ActorSystem)
    extends TestKit(_system)
    with AnyWordSpecLike
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers
    with CassandraLifecycle {

  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  lazy val sessionRegistry: CassandraSessionRegistry = CassandraSessionRegistry.get(system)

}
