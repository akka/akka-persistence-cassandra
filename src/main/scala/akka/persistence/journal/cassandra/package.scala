package akka.persistence.journal

import java.util.concurrent.Executor

import scala.concurrent._
import scala.util.Try

import com.google.common.util.concurrent.ListenableFuture

package object cassandra {
  implicit def listenableFutureToFuture[A](lf: ListenableFuture[A])(implicit executionContext: ExecutionContext): Future[A] = {
    val promise = Promise[A]
    lf.addListener(new Runnable { def run() = promise.complete(Try(lf.get())) }, executionContext.asInstanceOf[Executor])
    promise.future
  }
}
