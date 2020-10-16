/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

// FIXME this will be in akka-persistence-query

object AllEventsOffset {
  val empty: AllEventsOffset = AllEventsOffset(Map.empty)
}

final case class AllEventsOffset(offsets: Map[String, Long]) extends Offset
