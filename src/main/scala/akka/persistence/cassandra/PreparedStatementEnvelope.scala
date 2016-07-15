/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import akka.actor.NoSerializationVerificationNeeded
import com.datastax.driver.core.{PreparedStatement, Session}

case class PreparedStatementEnvelope(session:Session, ps:PreparedStatement) extends NoSerializationVerificationNeeded
