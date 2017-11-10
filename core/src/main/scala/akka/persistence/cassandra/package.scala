/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence

import java.util.concurrent.Executor

import com.google.common.util.concurrent.ListenableFuture

import scala.concurrent._
import scala.language.implicitConversions
import scala.util.Try

package object cassandra {
  // TODO we should use the more explicit ListenableFutureConverter asScala instead
  implicit def listenableFutureToFuture[A](lf: ListenableFuture[A])(implicit executionContext: ExecutionContext): Future[A] = {
    val promise = Promise[A]
    lf.addListener(new Runnable { def run() = promise.complete(Try(lf.get())) }, executionContext.asInstanceOf[Executor])
    promise.future
  }

  implicit class ListenableFutureConverter[A](val lf: ListenableFuture[A]) extends AnyVal {
    def asScala(implicit ec: ExecutionContext): Future[A] = {
      val promise = Promise[A]
      lf.addListener(new Runnable { def run() = promise.complete(Try(lf.get())) }, ec.asInstanceOf[Executor])
      promise.future
    }
  }

}
