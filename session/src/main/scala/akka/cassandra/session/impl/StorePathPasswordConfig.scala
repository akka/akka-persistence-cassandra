package akka.cassandra.session.impl

import akka.annotation.InternalApi

/**
  * INTERNAL API
  */
@InternalApi private[session] case class StorePathPasswordConfig(path: String, password: String)
