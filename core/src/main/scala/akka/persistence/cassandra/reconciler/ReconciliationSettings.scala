/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import com.typesafe.config.Config

final class ReconciliationSettings(config: Config) {

  val readProfile: String = config.getString("read-profile")
  val writeProfile: String = config.getString("write-profile")
  val pluginLocation: String = config.getString("plugin-location")

}
