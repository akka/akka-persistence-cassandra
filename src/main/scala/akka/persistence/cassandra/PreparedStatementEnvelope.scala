package akka.persistence.cassandra

import akka.actor.NoSerializationVerificationNeeded
import com.datastax.driver.core.{PreparedStatement, Session}

/**
  * Created with IntelliJ IDEA.
  * User: kasper
  * Date: 14-07-16
  * Time: 22:29
  */
case class PreparedStatementEnvelope(session:Session, ps:PreparedStatement) extends NoSerializationVerificationNeeded
