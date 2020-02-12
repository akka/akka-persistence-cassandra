/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.cassandra.CassandraSessionSettings;
import akka.stream.alpakka.cassandra.CassandraWriteSettings;
import akka.stream.alpakka.cassandra.javadsl.CassandraFlow;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry;
import akka.stream.alpakka.cassandra.javadsl.CassandraSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class CassandraFlowTest {
    static final String TEST_NAME = "CassandraFlowTest";

    static CassandraTestHelper helper;

    @BeforeClass
    public static void beforeAll() {
        helper = new CassandraTestHelper(TEST_NAME);
    }

    @AfterClass
    public static void afterAll() {
        helper.shutdown();
    }

    CassandraSessionRegistry sessionRegistry = CassandraSessionRegistry.get(helper.system);
    CassandraSessionSettings sessionSettings = CassandraSessionSettings.create("alpakka.cassandra");
    CassandraSession cassandraSession = sessionRegistry.sessionFor(sessionSettings, helper.system.dispatcher());

    @Test
    public void simpleUpdate() throws InterruptedException, ExecutionException, TimeoutException {
        String table = helper.createTableName();
        helper.cqlSession.execute("CREATE TABLE IF NOT EXISTS " + table + " (id int PRIMARY KEY);");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

        CompletionStage<Done> written = Source
                .from(data)
                .via(
                        CassandraFlow.create(
                                cassandraSession,
                                CassandraWriteSettings.defaults(),
                                "INSERT INTO " + table + "(id) VALUES (?)",
                                (element, preparedStatement) -> preparedStatement.bind(element)))
                .runWith(Sink.ignore(), helper.materializer);

        assertThat(written.toCompletableFuture().get(4, TimeUnit.SECONDS), is(Done.done()));

        CompletionStage<List<Integer>> select = CassandraSource.create(cassandraSession, "SELECT * FROM " + table).map(row -> row.getInt("id")).runWith(Sink.seq(), helper.materializer);
        List<Integer> rows = select.toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(new ArrayList<>(rows), hasItems(data.toArray()));
    }

}
