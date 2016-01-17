/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import java.util.concurrent.Executor

import com.google.common.util.concurrent.ListenableFuture

import scala.concurrent._
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

package object cassandra {
  implicit def listenableFutureToFuture[A](lf: ListenableFuture[A])(implicit executionContext: ExecutionContext): Future[A] = {
    val promise = Promise[A]
    lf.addListener(new Runnable { def run() = promise.complete(Try(lf.get())) }, executionContext.asInstanceOf[Executor])
    promise.future
  }
  def retry[T](n: Int)(fn: => T): T = {
    retry(n, 0)(fn)
  }

  @annotation.tailrec
  def retry[T](n: Int, delay: Long)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if n > 1 => {
        Thread.sleep(delay)
        retry(n - 1, delay)(fn)
      }
      case Failure(e) => throw e
    }
  }
}
