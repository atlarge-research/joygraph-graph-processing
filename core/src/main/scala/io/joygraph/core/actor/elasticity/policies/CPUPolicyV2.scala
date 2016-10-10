package io.joygraph.core.actor.elasticity.policies

import akka.cluster.metrics.StandardMetrics.Cpu
import io.joygraph.core.actor.metrics.WorkerOperation
import io.joygraph.core.message.AddressPair

// uses SIGAR to get the cpuCombined %
class CPUPolicyV2 extends DefaultAveragingPolicy[Double] {

  println("CPUPolicyV2")

  /**
    * We can only make a decision if we have metric data for each worker available
    */
  override def enoughInformationToMakeDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Boolean = {
    individualWorkerValues(currentStep, currentWorkers.keys).size == currentWorkers.size
  }

  override protected[this] def calculateAverage(step: Int, currentWorkers: Iterable[Int]): Option[Double] = {
    Some(ElasticPolicy.average(individualWorkerValues(step, currentWorkers).map(_._2)))
  }

  override protected[this] def individualWorkerValues(step: Int, currentWorkers: Iterable[Int]): Iterable[(Int, Double)] = {
    // get average CPU over all workers during the running of the step
    currentWorkers.flatMap { workerId =>
      // get processing time
      val metrics = metricsOf(step, workerId, WorkerOperation.RUN_SUPERSTEP).get
      val values = metrics.flatMap{
        case Cpu(address, timestamp, systemLoadAverage, cpuCombined, cpuStolen, processors) =>
          cpuCombined
      }

      if (values.isEmpty) {
        None
      } else {
        Some(workerId -> ElasticPolicy.average(values))
      }
    }
  }

  /**
    * Returns 1, if it has a positive impact given the evaluation function, negative impact -1 and there was no decision 0
    */
  override protected[this] def evaluatePreviousDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Int = {
    // check if decision exists
    decision(previousStep(currentStep)) match {
      case Some(x) =>
        if (stepAverage(currentStep).get >= stepAverage(previousStep(currentStep)).get) {
          -1
        } else {
          1
        }
      case None =>
        0
    }
  }
}
