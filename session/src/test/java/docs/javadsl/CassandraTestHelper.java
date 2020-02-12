/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.cassandra.CassandraSessionSettings;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry;
import akka.stream.alpakka.cassandra.scaladsl.CassandraAccess;
import akka.testkit.javadsl.TestKit;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class CassandraTestHelper {
    final ActorSystem system;
    final Materializer materializer;
    final CassandraSession cassandraSession;
    final CassandraAccess cassandraAccess;
    final String keyspaceName;
    final AtomicInteger tableNumber = new AtomicInteger();

    public CassandraTestHelper(String TEST_NAME) {
        system = ActorSystem.create(TEST_NAME);
        materializer = ActorMaterializer.create(system);
        CassandraSessionRegistry sessionRegistry = CassandraSessionRegistry.get(system);
        CassandraSessionSettings sessionSettings = CassandraSessionSettings.create("alpakka.cassandra");
        cassandraSession = sessionRegistry.sessionFor(sessionSettings, system.dispatcher());

        cassandraAccess = new CassandraAccess(cassandraSession.delegate());
        keyspaceName = TEST_NAME + System.nanoTime();
        try {
            Await.result(cassandraAccess.createKeyspace(keyspaceName), FiniteDuration.create(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        // `cassandraAccess` uses the system dispatcher through `cassandraSession`
        cassandraAccess.dropKeyspace(keyspaceName);
        TestKit.shutdownActorSystem(system);
    }

    public String createTableName() {
        return keyspaceName + "." + "test" + tableNumber.incrementAndGet();
    }

    public static <T> T await(CompletionStage<T> cs) throws InterruptedException, ExecutionException, TimeoutException {
        return cs.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

}
