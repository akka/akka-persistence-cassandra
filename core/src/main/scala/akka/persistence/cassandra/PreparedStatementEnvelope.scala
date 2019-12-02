/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.NoSerializationVerificationNeeded
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Session }

case class PreparedStatementEnvelope(session: Session, ps: PreparedStatement) extends NoSerializationVerificationNeeded
