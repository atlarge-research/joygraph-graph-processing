package io.joygraph.core.program

import scala.collection.mutable

trait Aggregatable {
  private[this] val _aggregators : scala.collection.mutable.Map[String, Aggregator[_]] = mutable.OpenHashMap.empty
  private[this] var _globalAggregators : Map[String, Aggregator[_]] = _

  def workerInitializeAggregators() = {
    initializeAggregators()
    _globalAggregators = _aggregators.toMap
  }
  protected[this] def initializeAggregators(): Unit

  protected[this] def aggregate[T](name : String, value : T) = {
    _aggregators(name).asInstanceOf[Aggregator[T]].aggregate(value)
  }

  protected[this] def aggregator[T](name : String, aggregator : Aggregator[T]) = {
    _aggregators(name) = aggregator
  }

  def printAggregatedValues() : Unit = _aggregators.foreach{
    case (name, aggregator) => println(s"$name: ${aggregator.value}")
  }

  def globalAggregators(globalAggregators : Map[String, Aggregator[_]]) = {
    _globalAggregators = globalAggregators
  }

  def aggregatedValue[T](name : String) : T = {
    _globalAggregators(name).value.asInstanceOf[T]
  }

  def aggregators() : Map[String, Aggregator[_]] = _aggregators.toMap

}