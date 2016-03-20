package io.joygraph.programs

import io.joygraph.core.program._

class BFSAggregatable extends BFS with Aggregatable {
  override def run(v: Vertex[Long, Long, NullClass], messages : Iterable[Long], superStep : Int): Boolean = {
    aggregate("maxMySteps", superStep)
    super.run(v,messages, superStep)
  }

  override def initializeAggregators(): Unit = {
    aggregator("maxMySteps", new Aggregator[Long] {
      private[this] var _aggregatedValue: Long = 0

      /**
        * this method must be thread-safe
        *
        * @param other value to aggregate
        */
      override def aggregate(other: Long): Unit = synchronized {
        _aggregatedValue = math.max(_aggregatedValue, other)
      }

      override def value: Long = _aggregatedValue
    })
  }

}
