package your.pack

import akka.stream.alpakka.cassandra.CqlSessionProvider
import com.datastax.dse.driver.api.core.DseSession
import com.datastax.oss.driver.api.core.CqlSession

import scala.jdk.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }

//#dse-session-provider
class DseSessionProvider extends CqlSessionProvider {
  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] = {
    DseSession
      .builder()
      // .withAuthProvider() can add any DSE specific authentication here
      .buildAsync()
      .asScala
  }
}
//#dse-session-provider
