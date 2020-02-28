/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #init-session
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.alpakka.cassandra.CassandraSessionSettings;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry;
// #init-session
// #cql
import akka.stream.alpakka.cassandra.javadsl.CassandraSource;
// #cql
import akka.stream.alpakka.cassandra.scaladsl.CassandraAccess;
import akka.stream.javadsl.Sink;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static docs.javadsl.CassandraTestHelper.await;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class CassandraSourceTest {
    static final String TEST_NAME = "CassandraSourceTest";

    static CassandraTestHelper helper;

    static String idtable;
    static final List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

    @BeforeClass
    public static void beforeAll() throws InterruptedException, ExecutionException, TimeoutException {

        helper = new CassandraTestHelper(TEST_NAME);
        idtable = helper.keyspaceName + ".inttable";
        await(helper.cassandraAccess.withSchemaMetadataDisabled(() ->
                helper.cassandraAccess.lifecycleSession().executeDDL("CREATE TABLE IF NOT EXISTS " + idtable + " (id int PRIMARY KEY);")
        ));
        await(helper.cassandraAccess.executeCqlList(data.stream().map(i -> "INSERT INTO " + idtable + "(id) VALUES (" + i + ")").collect(Collectors.toList())));
    }

    @AfterClass
    public static void afterAll() {
        helper.shutdown();
    }

    CassandraSession cassandraSession = helper.cassandraSession;
    CassandraAccess cassandraAccess = helper.cassandraAccess;

    @Test
    public void createSession() throws InterruptedException, ExecutionException, TimeoutException {
        // #init-session

        ActorSystem system = // ???
                // #init-session
                helper.system;
        // #init-session
        Materializer materializer = // ???
                // #init-session
                helper.materializer;
        // #init-session
        CassandraSessionSettings sessionSettings = CassandraSessionSettings.create();
        CassandraSession cassandraSession  =
                CassandraSessionRegistry.get(system).sessionFor(sessionSettings, system.dispatcher());

        CompletionStage<String> version =
                cassandraSession
                        .select("SELECT release_version FROM system.local;")
                        .map(row -> row.getString("release_version"))
                        .runWith(Sink.head(), materializer);
        // #init-session
        assertThat(await(version).isEmpty(), is(false));
    }

    @Test
    public void select() throws InterruptedException, ExecutionException, TimeoutException {
        Materializer materializer = helper.materializer;
        // #cql
        CompletionStage<List<Integer>> select =
                CassandraSource
                        .create(cassandraSession, "SELECT id FROM " + idtable + ";")
                        .map(r -> r.getInt("id"))
                        .runWith(Sink.seq(), materializer);
        // #cql
        List<Integer> rows = await(select);

        assertThat(new ArrayList<>(rows), hasItems(data.toArray()));
    }

    @Test
    public void selectVarArgs() throws InterruptedException, ExecutionException, TimeoutException {
        Materializer materializer = helper.materializer;
        int value = 5;
        // #cql

        CompletionStage<Integer> select =
                CassandraSource
                        .create(cassandraSession, "SELECT * FROM " + idtable + " WHERE id = ?;", value)
                        .map(r -> r.getInt("id"))
                        .runWith(Sink.head(), materializer);
        // #cql
        assertThat(await(select), is(value));
    }

    @Test
    public void statement() throws InterruptedException, ExecutionException, TimeoutException {
        Materializer materializer = helper.materializer;
        int value = 5;
        // #statement
        Statement<?> stmt = SimpleStatement.newInstance("SELECT * FROM " + idtable + ";").setPageSize(20);

        CompletionStage<List<Integer>> select =
                CassandraSource
                        .create(cassandraSession, stmt)
                        .map(r -> r.getInt("id"))
                        .runWith(Sink.seq(), materializer);
        // #statement
        assertThat(new ArrayList<>(await(select)), hasItems(data.toArray()));
    }

}
