/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import java.util.concurrent.atomic.AtomicLong

import akka.actor._

import com.codahale.metrics.MetricRegistry
import scala.concurrent.{ ExecutionContext, Future }
import scala.collection.convert.wrapAsScala._
import ExecutionContext.Implicits.global

/**
 * Retrieves Cassandra metrics registry for an actor system
 */
class CassandraMetricsRegistry extends Extension {
  private val metricRegistry = new MetricRegistry()

  def getRegistry: MetricRegistry = metricRegistry

  private[cassandra] def addMetrics(category: String, registry: MetricRegistry): Unit =
    metricRegistry.register(category, registry)

  private[cassandra] def removeMetrics(category: String): Unit =
    Future {
      metricRegistry.getNames.toList.filter(_.startsWith(category)).foreach(metricRegistry.remove)
    }
}

object CassandraMetricsRegistry
  extends ExtensionId[CassandraMetricsRegistry]
  with ExtensionIdProvider {
  override def lookup = CassandraMetricsRegistry
  override def createExtension(system: ExtendedActorSystem) = new CassandraMetricsRegistry
  override def get(system: ActorSystem): CassandraMetricsRegistry = super.get(system)
}