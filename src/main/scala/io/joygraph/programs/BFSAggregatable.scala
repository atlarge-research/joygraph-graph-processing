package io.joygraph.programs

import io.joygraph.core.program._

class BFSAggregatable extends BFS with Aggregatable {
  override def run(v: Vertex[Long, Int, NullClass, Int], messages : Iterable[Int], superStep : Int): Boolean = {
    aggregate("maxMySteps", superStep)
    super.run(v,messages, superStep)
  }

  override def initializeAggregators(): Unit = {
    aggregator("maxMySteps", new Aggregator[Int] {
      private[this] var _aggregatedValue: Int = 0

      /**
        * this method must be thread-safe
        *
        * @param other value to aggregate
        */
      override def aggregate(other: Int): Unit = synchronized {
        _aggregatedValue = math.max(_aggregatedValue, other)
      }

      override def value: Int = _aggregatedValue
    })
  }

}
