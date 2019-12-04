/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra.session

// FIXME probably get rid of this
object CassandraSessionSettings {
  def apply(profile: String): CassandraSessionSettings =
    new CassandraSessionSettings(profile)
}

class CassandraSessionSettings(val profile: String)
