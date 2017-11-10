/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SequenceNumbers {
  val empty = SequenceNumbers(Map.empty, Map.empty)

  sealed trait Answer
  case object Yes extends Answer
  case object Before extends Answer
  case object After extends Answer
  case object PossiblyFirst extends Answer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class SequenceNumbers(
  intNumbers: Map[String, Int], longNumbers: Map[String, Long]
) {
  import SequenceNumbers._

  def isNext(persistenceId: String, seqNr: Long): Answer = {
    val n = get(persistenceId)
    if (seqNr == n + 1) Yes
    else if (n == 0) PossiblyFirst
    else if (seqNr > n + 1) After
    else Before
  }

  def get(persistenceId: String): Long =
    intNumbers.get(persistenceId) match {
      case Some(n) => n.toLong
      case None => longNumbers.get(persistenceId) match {
        case Some(n2) => n2
        case None     => 0L
      }
    }

  def updated(persistenceId: String, seqNr: Long): SequenceNumbers =
    if (seqNr <= Int.MaxValue)
      copy(intNumbers = intNumbers.updated(persistenceId, seqNr.toInt))
    else if (seqNr == 1L + Int.MaxValue)
      copy(
        intNumbers = intNumbers - persistenceId,
        longNumbers = longNumbers.updated(persistenceId, seqNr)
      )
    else
      copy(longNumbers = longNumbers.updated(persistenceId, seqNr))

}
