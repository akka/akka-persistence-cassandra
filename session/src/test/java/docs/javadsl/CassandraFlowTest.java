/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.cassandra.CassandraWriteSettings;
import akka.stream.alpakka.cassandra.javadsl.CassandraFlow;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static docs.javadsl.CassandraTestHelper.await;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
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

    CassandraSession cassandraSession = helper.cassandraSession;

    @Test
    public void simpleUpdate() throws InterruptedException, ExecutionException, TimeoutException {
        String table = helper.createTableName();
        await(cassandraSession.executeDDL("CREATE TABLE IF NOT EXISTS " + table + " (id int PRIMARY KEY);"));
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

        assertThat(await(written), is(Done.done()));

        CompletionStage<List<Integer>> select = CassandraSource
                .create(cassandraSession, "SELECT * FROM " + table)
                .map(row -> row.getInt("id"))
                .runWith(Sink.seq(), helper.materializer);
        List<Integer> rows = await(select);
        assertThat(new ArrayList<>(rows), hasItems(data.toArray()));
    }


    @Test
    public void typedUpdate() throws InterruptedException, ExecutionException, TimeoutException {
        String table = helper.createTableName();
        await(cassandraSession.executeDDL("CREATE TABLE IF NOT EXISTS " + table + " (id int PRIMARY KEY, name text, city text);"));

        List<Person> persons = Arrays.asList(
                new Person(12, "John", "London"),
                new Person(43, "Umberto", "Roma"),
                new Person(56, "James", "Chicago")
        );

        CompletionStage<Done> written = Source.from(persons)
                .via(CassandraFlow.create(
                        cassandraSession,
                        CassandraWriteSettings.defaults(),
                        "INSERT INTO " + table + "(id, name, city) VALUES (?, ?, ?)",
                        (person, preparedStatement) -> preparedStatement.bind(person.id, person.name, person.city)
                ))
                .runWith(Sink.ignore(), helper.materializer);

        assertThat(await(written), is(Done.done()));

        CompletionStage<List<Person>> select = CassandraSource.create(cassandraSession, "SELECT * FROM " + table)
                .map(row -> new Person(row.getInt("id"), row.getString("name"), row.getString("city")))
                .runWith(Sink.seq(), helper.materializer);
        List<Person> rows = await(select);
        assertThat(new ArrayList<>(rows), hasItems(persons.toArray()));
    }

    public static final class Person {
        public final int id;
        public final String name;
        public final String city;

        public Person(int id, String name, String city) {
            this.id = id;
            this.name = name;
            this.city = city;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Person that = (Person) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(city, that.city);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, city);
        }
    }

}
