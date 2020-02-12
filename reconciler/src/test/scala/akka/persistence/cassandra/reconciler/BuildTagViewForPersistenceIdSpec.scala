/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.cassandra.CassandraSpec

class BuildTagViewForPersisetceIdSpec extends CassandraSpec("""

akka.persistence.cassandra {
 events-by-tag {
   # enabled = false
 } 
}

""") {

  "BuildTagViewForPersistenceId" should {

    "build from scratch" in {
      pending

    }

    "build partially" in {
      pending

    }

  }
}
