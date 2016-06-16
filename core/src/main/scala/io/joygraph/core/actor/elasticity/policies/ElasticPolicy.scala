package io.joygraph.core.actor.elasticity.policies

import akka.actor.Address
import akka.cluster.metrics.NodeMetrics
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
  def default() : ElasticPolicy = (currentStep: Int, currentWorkers: Map[Int, AddressPair]) => None

  sealed abstract class Result
  case class Shrink(workersToRemove : Iterable[WorkerId], partitioner : VertexPartitioner) extends Result
  case class Grow(workersToAdd : Iterable[WorkerId], partitioner : VertexPartitioner) extends Result
}

abstract class ElasticPolicy {
  import ElasticPolicy.WorkerId

  private type SuperStep = Int
  private type WorkerMetrics = mutable.OpenHashMap[WorkerId, ArrayBuffer[NodeMetrics]]
  private[this] val _superStepMetrics = TrieMap.empty[SuperStep, WorkerMetrics]

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

  private[this] def superStepMetrics(superStep : SuperStep) : WorkerMetrics = {
    _superStepMetrics.getOrElseUpdate(superStep, new WorkerMetrics)
  }

  /**
    * @return Immutable metrics snapshot
    */
  protected[this] def superStepWorkerMetrics(superStep : SuperStep, workerId : WorkerId) : Iterable[NodeMetrics]= {
    superStepMetrics(superStep).getOrElse(workerId, ArrayBuffer.empty).toIndexedSeq
  }

  def decide(currentStep : Int, currentWorkers : Map[Int, AddressPair]) : Option[ElasticPolicy.Result]
}
