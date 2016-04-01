package io.joygraph.core.program


trait Aggregatable {
  private[this] var _aggregators : scala.collection.mutable.Map[String, Aggregator[_]] = scala.collection.mutable.OpenHashMap.empty
  private[this] var _globalAggregators : Map[String, Aggregator[_]] = _
  private[this] var _previousStepValues : Map[String, _] = _

  def workerInitializeAggregators() = {
    initializeAggregators()
    _globalAggregators = _aggregators.toMap
  }

  def saveAggregatedValues() = {
    _previousStepValues = _globalAggregators.mapValues(_.value)
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

  def previousStepAggregatedValue[T](name : String) : T = {
    _previousStepValues(name).asInstanceOf[T]
  }

  def aggregators() : Map[String, Aggregator[_]] = _aggregators.toMap

  protected def aggregatorsRef :  scala.collection.mutable.Map[String, Aggregator[_]] = _aggregators
  protected def globalAggregatorsRef : Map[String, Aggregator[_]] = _globalAggregators
  protected def previousStepValuesRef : Map[String, _] = _previousStepValues

  def copyReferencesFrom(aggregatable: Aggregatable) = {
    _aggregators = aggregatable.aggregatorsRef
    _globalAggregators = aggregatable.globalAggregatorsRef
    _previousStepValues = aggregatable.previousStepValuesRef
  }
}