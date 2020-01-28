/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.cassandra.session.CassandraSessionSettings;
import akka.cassandra.session.javadsl.CassandraSession;
import akka.cassandra.session.javadsl.CassandraSessionRegistry;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.cassandra.javadsl.CassandraSource;
import akka.stream.alpakka.cassandra.scaladsl.CassandraAccess;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CassandraSourceTest {
    static final String TEST_NAME = "CassandraSourceTest";

    static ActorSystem system;
    static Materializer materializer;
    static CqlSession cqlSession;
    static CassandraAccess cassandraAccess;
    static String keyspaceName;
    final static AtomicInteger tableNumber = new AtomicInteger();

    @BeforeClass
    public static void beforeAll() {
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

    @AfterClass
    public static void afterAll() {
        TestKit.shutdownActorSystem(system);
        cassandraAccess.dropKeyspace(keyspaceName);
        cqlSession.close();
    }

    String createTableName() {
        return keyspaceName + "." + "test" + tableNumber.incrementAndGet();
    }

    CassandraSessionRegistry sessionRegistry = CassandraSessionRegistry.get(system);
    CassandraSessionSettings sessionSettings = CassandraSessionSettings.create("alpakka.cassandra");
    CassandraSession cassandraSession = sessionRegistry.sessionFor(sessionSettings, system.dispatcher());

    @Test
    public void select() throws InterruptedException, ExecutionException, TimeoutException {
        String table = createTableName();
        cqlSession.execute("CREATE TABLE IF NOT EXISTS " + table + " (id int PRIMARY KEY);");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        cassandraAccess.executeCqlList(data.stream().map(i -> "INSERT INTO " + table + "(id) VALUES (" + i + ")").collect(Collectors.toList()));

        List<Row> rows = CassandraSource.create(cassandraSession, "SELECT * FROM " + table).runWith(Sink.seq(), materializer).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(rows.stream().map(r -> r.getInt("id")).collect(Collectors.toList()), hasItems(data.toArray()));
    }

}
