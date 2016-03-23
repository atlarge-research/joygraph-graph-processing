package io.joygraph.core.program

abstract class Aggregator[T] extends Serializable {

  /**
    * this method must be thread-safe
    *
    * @param other value to aggregate
    */
  def aggregate(other : T) : Unit
  def aggregate(aggregator : Aggregator[_]) : Unit = aggregate(aggregator.value.asInstanceOf[T])
  def value : T

  /**
    * (if persistent do not reset value here)
    * Optional workerPrepareStep hook
    */
  def workerPrepareStep() : Unit = {}

  /**
    * (if persistent do not reset value here)
    * Optional masterPrepareStep hook
    */
  def masterPrepareStep() : Unit = {}
}
