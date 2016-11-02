package io.joygraph.analysis

import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.Result
import io.joygraph.core.actor.metrics.{WorkerOperation, WorkerState, WorkerStateRecorder}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

case class MetricsTransformer(metricsFilePath : String) {
  val policyMetricsReader : ElasticPolicyReader = ElasticPolicyReader(metricsFilePath)
}

case class ElasticPolicyReader(filePath : String) extends ElasticPolicy {
  importMetrics(filePath)

  def totalNumberOfSteps() : Int = {
    _activeVerticesEndOfStep.keys.max
  }

  def workersForStep(step : Int) : Iterable[Int] = {
    _activeVerticesEndOfStep(step).keys
  }

  // proxy function for statesrecorder
  private[this] def statesFor(step : Int, statesPerStep: TrieMap[Int, mutable.Map[Int, mutable.Map[WorkerOperation.Value, WorkerState]]]) = {
    statesPerStep.withDefault(Map.empty)(step).toMap
  }

  private[this] def statesPerStep(): TrieMap[Int, mutable.Map[Int, mutable.Map[WorkerOperation.Value, WorkerState]]] = {
    // use reflection to get raw ...
    val workerStateRecorderClazz = classOf[WorkerStateRecorder]
    val field = workerStateRecorderClazz.getDeclaredField("statesPerStep")
    field.setAccessible(true)
    field.get(_statesRecorder).asInstanceOf[TrieMap[Int, mutable.Map[Int, mutable.Map[WorkerOperation.Value, WorkerState]]]]
  }

  def rawStates(operation : WorkerOperation.Value): IndexedSeq[Iterable[WorkerState]] = {
    val states = for (step <- 0 until totalNumberOfSteps()) yield {
      statesFor(step, statesPerStep()).flatMap{
        case (workerId, operationsMap) =>
          operationsMap.get(operation)
      }
    }
    states
  }

  def timeOfAllWorkersOfAction(step : Int, operation : WorkerOperation.Value): Long = {
    statesFor(step, statesPerStep()).flatMap {
      case (workerId, operationsMap) =>
        operationsMap.get(operation) match {
          case Some(WorkerState(_, start, stopOpt)) =>
            if (stopOpt.isEmpty) {
              None
            } else {
              Some(stopOpt.get - start)
            }
          case None =>
            None
        }
    }.sum
  }

  override def init(policyParams: Config): Unit = ???
  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = ???
}