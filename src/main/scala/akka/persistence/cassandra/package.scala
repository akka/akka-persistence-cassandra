/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import com.google.common.util.concurrent.{ FutureCallback, Futures, ListenableFuture }

import scala.concurrent._
import scala.language.implicitConversions

package object cassandra {

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
