package io.joygraph.core.actor.elasticity.policies

import akka.actor.Address
import akka.cluster.metrics.NodeMetrics
import com.typesafe.config.Config
import io.joygraph.core.actor.metrics.{WorkerOperation, WorkerState, WorkerStateRecorder}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ElasticPolicy {
  protected type WorkerId = Int
  protected type NumWorkers = Int

  /**
    * @return Default policy which does not do anything
    */
  def default() : ElasticPolicy = new ElasticPolicy {
    override def init(policyParams: Config): Unit = {}

    override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner): Option[Result] = None
  }

  sealed abstract class Result
  case class Shrink(workersToRemove : Iterable[WorkerId], partitioner : VertexPartitioner) extends Result
  case class Grow(workersToAdd : Iterable[WorkerId], partitioner : VertexPartitioner) extends Result

  /**
    * @param extractor extracts a metric T from NodeMetrics
    * @param sumFunction a function which sums an Iterable of T
    * @tparam T type of metric
    */
  def singleWorkerAverage[T]
  (nodeMetricsIterable : Iterable[NodeMetrics],
   extractor : NodeMetrics => T,
   sumFunction : Iterable[T] => T)(implicit x : Fractional[T]) : T = {
    val extractedMetrics = nodeMetricsIterable.map(extractor)
    average(extractedMetrics)
  }

  def singleWorkerAverageOption[T]
  (nodeMetricsIterable : Iterable[NodeMetrics],
   extractor : NodeMetrics => Option[T],
   sumFunction : Iterable[T] => T)(implicit x : Fractional[T]) : T= {
    val extractedMetrics = nodeMetricsIterable.map(extractor).flatten
    average(extractedMetrics)
  }

  def average[T](nums : Iterable[T])(implicit x : Fractional[T]): T = {
    x.div(nums.sum, x.fromInt(nums.size))
  }

  def standardDeviation[T](nums : Iterable[T], averageOpt : Option[T])(implicit x : Fractional[T]) : Double = {
    val avg = averageOpt.getOrElse(average(nums))
    math.sqrt(nums.map{
      num =>
        x.toDouble(
          x.times(x.minus(num, avg), x.minus(num, avg))
        )
    }.sum / nums.size)
  }

}

abstract class ElasticPolicy {
  import ElasticPolicy.WorkerId

  private type SuperStep = Int
  private type WorkerMetrics = mutable.OpenHashMap[WorkerId, ArrayBuffer[NodeMetrics]]
  private[this] val _superStepMetrics = TrieMap.empty[SuperStep, WorkerMetrics]
  private[this] var _statesRecorder : WorkerStateRecorder = _

  final def addMetrics(superStep : Int, metricsSet : Set[NodeMetrics], addressToWorkerMap : Map[Address, WorkerId]): Unit = synchronized {
    val workerMetrics = _superStepMetrics.getOrElseUpdate(superStep, new WorkerMetrics)
    metricsSet.foreach{
      case nodeMetrics @ NodeMetrics(address, timestamp, metrics) =>
        addressToWorkerMap.get(address) match {
          case Some(workerId) =>
            workerMetrics.getOrElseUpdate(workerId, ArrayBuffer.empty[NodeMetrics]) += nodeMetrics
          case None =>
            // noop
        }
    }
  }

  /**
    * Retrieve raw metrics for combination of superstep, workerId, and state
    * @return
    */
  protected[this] def metricsOf(superStep : Int, workerId : Int, state : WorkerOperation.Value): Option[ArrayBuffer[NodeMetrics]] = {
    // find worker
    val states = _statesRecorder.statesFor(superStep, workerId)
    states.get(state) match {
      case Some(WorkerState(_, startTime, stopOpt)) =>
        stopOpt match {
          case Some(stopTime) =>
            Some(superStepMetrics(superStep)(workerId).filter(x => x.timestamp >= startTime && x.timestamp <= stopTime))
          case None =>
            Some(superStepMetrics(superStep)(workerId).filter(x => x.timestamp >= startTime))
        }
      case None => None
    }
  }

  /**
    * Processing time in ms
    */
  protected def processingTime(superStep : SuperStep, workerId : WorkerId) : Long = {
    val timestamps = superStepWorkerMetrics(superStep, workerId).map(_.timestamp)
    timestamps.max - timestamps.min
  }

  private[this] def superStepMetrics(superStep : SuperStep) : WorkerMetrics = {
    _superStepMetrics.getOrElseUpdate(superStep, new WorkerMetrics)
  }

  /**
    * @return Immutable metrics snapshot
    */
  protected[this] def superStepWorkerMetrics(superStep : SuperStep, workerId : WorkerId) : Iterable[NodeMetrics]= {
    superStepMetrics(superStep).getOrElse(workerId, ArrayBuffer.empty).toIndexedSeq
  }

  final def init(policyParams : Config, statesRecorder : WorkerStateRecorder) : Unit = {
    _statesRecorder = statesRecorder
    init(policyParams)
  }

  def init(policyParams : Config) : Unit

  def decide(currentStep : Int, currentWorkers : Map[Int, AddressPair], currentPartitioner : VertexPartitioner) : Option[ElasticPolicy.Result]

}
