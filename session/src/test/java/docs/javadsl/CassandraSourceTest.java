/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSource;
import akka.stream.alpakka.cassandra.scaladsl.CassandraAccess;
import akka.stream.javadsl.Sink;
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
import java.util.stream.Collectors;

import static docs.javadsl.CassandraTestHelper.await;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

public class CassandraSourceTest {
    static final String TEST_NAME = "CassandraSourceTest";

    static CassandraTestHelper helper;

    @BeforeClass
    public static void beforeAll() {
        helper = new CassandraTestHelper(TEST_NAME);
    }

    @AfterClass
    public static void afterAll() {
        helper.shutdown();
    }

    CassandraSession cassandraSession = helper.cassandraSession;
    CassandraAccess cassandraAccess = helper.cassandraAccess;

    @Test
    public void select() throws InterruptedException, ExecutionException, TimeoutException {
        String table = helper.createTableName();
        await(cassandraAccess.withSchemaMetadataDisabled(() ->
            cassandraAccess.lifecycleSession().executeDDL("CREATE TABLE IF NOT EXISTS " + table + " (id int PRIMARY KEY);")
        ));
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        await(helper.cassandraAccess.executeCqlList(data.stream().map(i -> "INSERT INTO " + table + "(id) VALUES (" + i + ")").collect(Collectors.toList())));

        CompletionStage<List<Integer>> select = CassandraSource.create(cassandraSession, "SELECT * FROM " + table).map(r -> r.getInt("id")).runWith(Sink.seq(), helper.materializer);
        List<Integer> rows = select.toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(new ArrayList<>(rows), hasItems(data.toArray()));
    }

}
