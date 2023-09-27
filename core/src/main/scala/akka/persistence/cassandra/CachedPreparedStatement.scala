/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.annotation.InternalApi
import akka.util.OptionVal
import com.datastax.oss.driver.api.core.cql.PreparedStatement

/**
 * INTERNAL API
 */
@InternalApi private[cassandra] class CachedPreparedStatement(init: () => Future[PreparedStatement])(
    implicit val ec: ExecutionContext) {
  @volatile private var preparedStatement: OptionVal[Future[PreparedStatement]] = OptionVal.None

  def get(): Future[PreparedStatement] =
    preparedStatement match {
      case OptionVal.Some(ps) => ps
      case _                  =>
        // ok to init multiple times in case of concurrent access
        val ps = init()
        ps.foreach { p =>
          // only cache successful futures, ok to overwrite
          preparedStatement = OptionVal.Some(Future.successful(p))
        }
        ps
    }

}
