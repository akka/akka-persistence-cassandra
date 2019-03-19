/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra

import java.util.concurrent.Executor

import akka.Done
import com.google.common.util.concurrent.ListenableFuture

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.Try

package object session {

  val FutureDone: Future[Done] = Future.successful(Done)

  implicit class ListenableFutureConverter[A](val lf: ListenableFuture[A]) extends AnyVal {
    def asScala(implicit ec: ExecutionContext): Future[A] = {
      val promise = Promise[A]
      lf.addListener(new Runnable {
        def run() = promise.complete(Try(lf.get()))
      }, ec.asInstanceOf[Executor])
      promise.future
    }
  }

}
