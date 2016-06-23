/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import java.util.concurrent.Executor

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent._
import scala.language.implicitConversions
import scala.util.Try

package object cassandra {
  implicit def listenableFutureToFuture[A](lf: ListenableFuture[A])(implicit executionContext: ExecutionContext): Future[A] = {
    val promise = Promise[A]
    lf.addListener(new Runnable { def run() = promise.complete(Try(lf.get())) }, executionContext.asInstanceOf[Executor])
    promise.future
  }

  implicit class ListenableFutureOps[V](lf: ListenableFuture[V]) {
    def asScala = {
      val p = Promise[V]()
      Futures.addCallback(lf, new FutureCallback[V] {
        override def onFailure(t: Throwable): Unit = p.failure(t)
        override def onSuccess(result: V): Unit = p.success(result)
      })
      p.future
    }
  }
}
