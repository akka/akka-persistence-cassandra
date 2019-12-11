/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.NoSerializationVerificationNeeded
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement

case class PreparedStatementEnvelope(session: CqlSession, ps: PreparedStatement)
    extends NoSerializationVerificationNeeded
