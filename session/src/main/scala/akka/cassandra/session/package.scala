/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra

import akka.Done

import scala.concurrent.Future

package object session {
  val FutureDone: Future[Done] = Future.successful(Done)
}
