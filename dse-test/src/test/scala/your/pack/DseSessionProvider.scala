package your.pack

import akka.cassandra.session.CqlSessionProvider
import com.datastax.dse.driver.api.core.DseSession
import com.datastax.oss.driver.api.core.CqlSession

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }

//#dse-session-provider
class DseSessionProvider extends CqlSessionProvider {
  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] = {
    DseSession
      .builder()
      // .withAuthProvider() can add any DSE specific authentication here
      .buildAsync()
      .toScala
  }
}
//#dse-session-provider
