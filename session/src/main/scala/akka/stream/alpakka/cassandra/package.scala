/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka

import akka.Done

import scala.concurrent.Future

package object cassandra {
  val FutureDone: Future[Done] = Future.successful(Done)
}
