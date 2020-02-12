/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.cassandra.scaladsl.CassandraAccess;
import akka.testkit.javadsl.TestKit;
import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class CassandraTestHelper {
     ActorSystem system;
     Materializer materializer;
     CqlSession cqlSession;
     CassandraAccess cassandraAccess;
     String keyspaceName;
    final  AtomicInteger tableNumber = new AtomicInteger();

    public CassandraTestHelper(String TEST_NAME) {
        system = ActorSystem.create(TEST_NAME);
        materializer = ActorMaterializer.create(system);
        cqlSession = CqlSession
                .builder()
                .withLocalDatacenter("datacenter1")
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .build();
        cassandraAccess = new CassandraAccess(cqlSession);
        keyspaceName = TEST_NAME+System.nanoTime();
        cassandraAccess.createKeyspace(keyspaceName);

    }

    public void shutdown() {
        TestKit.shutdownActorSystem(system);
        cassandraAccess.dropKeyspace(keyspaceName);
        cqlSession.close();
    }


    public String createTableName() {
        return keyspaceName + "." + "test" + tableNumber.incrementAndGet();
    }
}
