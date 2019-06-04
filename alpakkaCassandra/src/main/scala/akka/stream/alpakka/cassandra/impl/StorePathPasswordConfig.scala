/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.impl

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[cassandra] case class StorePathPasswordConfig(path: String, password: String)
