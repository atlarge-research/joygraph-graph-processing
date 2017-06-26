package io.joygraph.core.actor.elasticity.policies

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.actor.Address
import akka.cluster.metrics.NodeMetrics
import com.typesafe.config.Config
import io.joygraph.core.actor.metrics.{SupplyDemandMetrics, WorkerOperation, WorkerState, WorkerStateRecorder}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object ElasticPolicy {
  protected type WorkerId = Int
  protected type NumWorkers = Int

  /**
    * @return Default policy which does not do anything
    */
  def default() : ElasticPolicy = new ElasticPolicy {
    override def init(policyParams: Config): Unit = {}

    override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = None
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

  private type WorkerMetrics = mutable.HashMap[WorkerId, ArrayBuffer[NodeMetrics]]
  protected[this] var _superStepMetrics = TrieMap.empty[SuperStep, WorkerMetrics]
  protected[this] var _statesRecorder : WorkerStateRecorder = _
  protected[this] var _activeVerticesEndOfStep = TrieMap.empty[Int, scala.collection.mutable.Map[Int, Long]]
  protected[this] var _supplyDemandMetrics = ArrayBuffer.empty[SupplyDemandMetrics]
  protected[this] var _rawSupplyDemandMetrics = ArrayBuffer.empty[SupplyDemandMetrics]

  final def addRawSupplyDemand(supplyDemand : SupplyDemandMetrics) : Unit = synchronized {
    _rawSupplyDemandMetrics += supplyDemand
  }

  def rawSupplyDemands : ArrayBuffer[SupplyDemandMetrics] = {
    _rawSupplyDemandMetrics
  }

  final def addSupplyDemand(supplyDemand : SupplyDemandMetrics) : Unit = synchronized {
    _supplyDemandMetrics += supplyDemand
  }

  def supplyDemands : ArrayBuffer[SupplyDemandMetrics] = {
    _supplyDemandMetrics
  }

  final def addActiveVertices(step : Int, workerId : Int, num : Long) : Unit = {
    val workerMap = _activeVerticesEndOfStep.getOrElseUpdate(step, TrieMap.empty)
    workerMap.getOrElseUpdate(workerId, num)
  }

  final def workerSuperStepTimes(step : Int, workerIds : Iterable[Int]) : Iterable[(Int, Long)] = {
    workerIds.map {
      workerId =>
        // get superstep time
        workerId -> timeOfOperation(step, workerId, WorkerOperation.RUN_SUPERSTEP).get
    }
  }
  final def averageTimeOfStep(step : Int, workerIds : Iterable[Int]): Double = {
    ElasticPolicy.average[Double](workerSuperStepTimes(step, workerIds).map(_._2.toDouble))
  }

  def activeVerticesSumOf(step : Int) : Long = {
    _activeVerticesEndOfStep.get(step) match {
      case Some(x) =>
        x.values.sum
      case None =>
        0
    }
  }
  def activeVerticesOf(step : Int, workerId : Int) : Option[Long] = {
    _activeVerticesEndOfStep.get(step) match {
      case Some(x) =>
        x.get(workerId) match {
          case Some(y) => Some(y)
          case None => None
        }
      case None =>
        None
    }
  }

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
    *
    * @return
    */
  def metricsOf(superStep : Int, workerId : Int, state : WorkerOperation.Value): Option[ArrayBuffer[NodeMetrics]] = {
    // find worker
    val states = _statesRecorder.statesFor(superStep, workerId)
    states.get(state) match {
      case Some(WorkerState(_, startTime, stopOpt)) =>
        stopOpt match {
          case Some(stopTime) =>
            Some(superStepWorkerMetrics(superStep, workerId).filter(x => x.timestamp >= startTime && x.timestamp <= stopTime))
          case None =>
            Some(superStepWorkerMetrics(superStep, workerId).filter(x => x.timestamp >= startTime))
        }
      case None => None
    }
  }

  def stepTime(superStep : SuperStep, workerId : WorkerId) : Long = {
    val timestamps = superStepWorkerMetrics(superStep, workerId).map(_.timestamp)
    if (timestamps.isEmpty) {
      0
    } else {
      timestamps.max - timestamps.min
    }
  }

  def timeOfOperation(superStep : SuperStep, workerId : WorkerId, operation : WorkerOperation.Value): Option[Long] = {
    val WorkerState(_, start, stopOpt) = _statesRecorder.statesFor(superStep, workerId)(operation)
    stopOpt match {
      case Some(stop) =>
        Some(stop - start)
      case None =>
        None
    }
  }

  private[this] def superStepMetrics(superStep : SuperStep) : WorkerMetrics = {
    _superStepMetrics.getOrElseUpdate(superStep, new WorkerMetrics)
  }

  /**
    * @return Immutable metrics snapshot
    */
  def superStepWorkerMetrics(superStep : SuperStep, workerId : WorkerId) : ArrayBuffer[NodeMetrics]= {
    superStepMetrics(superStep).getOrElse(workerId, ArrayBuffer.empty)
  }

  def superStepMetricsAllWorkers(superStep : SuperStep): mutable.Map[WorkerId, ArrayBuffer[NodeMetrics]] = {
    superStepMetrics(superStep).withDefaultValue(ArrayBuffer.empty)
  }

  final def init(policyParams : Config, statesRecorder : WorkerStateRecorder) : Unit = {
    _statesRecorder = statesRecorder
    init(policyParams)
  }

  def init(policyParams : Config) : Unit

  def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int) : Option[ElasticPolicy.Result]

  def importMetrics(path : String) : Unit = {
    Try {
      new FileInputStream(path)
    } match {
      case Failure(exception) =>
        throw exception
      case Success(fis) =>
        Try {
          new ObjectInputStream(fis)
        } match {
          case Failure(exception) =>
            fis.close()
            throw exception
          case Success(ois) =>
            _superStepMetrics = ois.readObject().asInstanceOf[TrieMap[SuperStep, WorkerMetrics]]
            _statesRecorder = ois.readObject().asInstanceOf[WorkerStateRecorder]
            _activeVerticesEndOfStep = ois.readObject().asInstanceOf[TrieMap[Int, scala.collection.mutable.Map[Int, Long]]]
            _supplyDemandMetrics = ois.readObject().asInstanceOf[ArrayBuffer[SupplyDemandMetrics]]
            _rawSupplyDemandMetrics = ois.readObject().asInstanceOf[ArrayBuffer[SupplyDemandMetrics]]
        }
    }
  }

  def exportMetrics(path : String): Unit = {
    Try {
      new FileOutputStream(path)
    } match {
      case Failure(exception) =>
      case Success(fos) =>
        Try {
          new ObjectOutputStream(fos)
        } match {
          case Failure(exception) =>
            fos.close()
          case Success(oos) =>
            try {
              oos.writeObject(_superStepMetrics)
              oos.writeObject(_statesRecorder)
              oos.writeObject(_activeVerticesEndOfStep)
              oos.writeObject(_supplyDemandMetrics)
              oos.writeObject(_rawSupplyDemandMetrics)
            } finally {
              oos.close()
            }
        }
    }
  }
}
