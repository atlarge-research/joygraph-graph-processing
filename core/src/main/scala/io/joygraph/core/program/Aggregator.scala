package io.joygraph.core.program

abstract class Aggregator[T] extends Serializable {

  protected[this] var _aggregatedValue : T

  /**
    * this method must be thread-safe
    *
    * @param other value to aggregate
    */
  def aggregate(other : T) : Unit
  def aggregate(aggregator : Aggregator[_]) : Unit = aggregate(aggregator.value.asInstanceOf[T])
  def value : T = _aggregatedValue
}
