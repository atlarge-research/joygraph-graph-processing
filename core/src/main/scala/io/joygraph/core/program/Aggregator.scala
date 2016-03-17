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
    * Optional onStepComplete hook
    */
  def workerOnStepComplete() : Unit = {}

  /**
    * (if persistent do not reset value here)
    * Optional masterOnStepComplete hook
    */
  def masterOnStepComplete() : Unit = {}
}
