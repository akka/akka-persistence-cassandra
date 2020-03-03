/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

object SnapshotWithMetaData {

  /**
   * If meta data could not be deserialized it will not fail the query.
   * The "invalid" meta data is represented with this `UnknownMetaData` and
   * it and the event will be wrapped in `SnapshotWithMetaData`.
   *
   * The reason for not failing the query is that meta data should be
   * optional, e.g. the tool that wrote the meta data has been removed. This
   * is typically because the serializer for the meta data has been removed
   * from the class path (or configuration).
   */
  final case class UnknownMetaData(serializerId: Int, manifest: String)
}

/**
 * If the event is wrapped in this class the `metaData` will
 * be serialized and stored in the `meta` column. This can be used by event
 * adapters or other tools to store additional meta data without altering
 * the actual domain event.
 */
final case class SnapshotWithMetaData(event: Any, metaData: Any)
