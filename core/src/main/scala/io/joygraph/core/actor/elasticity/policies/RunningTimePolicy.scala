package io.joygraph.core.actor.elasticity.policies
import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Result, Shrink}
import io.joygraph.core.actor.metrics.WorkerOperation
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.ReHashPartitioner

import scala.collection.mutable

class RunningTimePolicy extends ElasticPolicy {
  val stepAverages = mutable.Map.empty[Int, Long]
  val vertexPartitioners = mutable.Map.empty[Int, VertexPartitioner]
  val decisions = mutable.Map.empty[Int, Option[Result]]

  override def init(policyParams: Config): Unit = {

  }

  protected[this] def revertPreviousDecision(currentStep : Int) : Option[Result] = {
    if (stepAverages(currentStep) >= stepAverages(currentStep - 1)) {
      // no improvement, revert
      decisions(currentStep - 1) match {
        case Some(result) =>
          result match {
            case Shrink(workersToRemove, partitioner) =>
              Some(Grow(workersToRemove, vertexPartitioners(currentStep - 1)))
            case Grow(workersToAdd, partitioner) =>
              Some(Shrink(workersToAdd, vertexPartitioners(currentStep - 1)))
          }
        case None =>
          None // nothing to revert
      }
    } else {
      None
    }
  }

  protected[this] def hasPreviousDecision(currentStep : Int): Boolean = {
    if (currentStep > 0) {
      decisions(currentStep - 1) match {
        case Some(_) =>
          true
        case None =>
          false
      }
    } else {
      false
    }
  }

  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
    val workerIdSuperStepTimes: Iterable[(Int, Long)] = currentWorkers.keys.map {
      workerId =>
        // get superstep time
        workerId -> timeOfOperation(currentStep, workerId, WorkerOperation.RUN_SUPERSTEP).get
    }

    val workerSuperStepTimes = workerIdSuperStepTimes.map(_._2.toDouble)
    val averageStepTime = ElasticPolicy.average(workerSuperStepTimes)
    vertexPartitioners(currentStep) = currentPartitioner
    stepAverages(currentStep) = averageStepTime.toLong

    // evaluate prev decision
    val decision =  if (hasPreviousDecision(currentStep)) {
      revertPreviousDecision(currentStep)
    } else {
      val standardDeviation = ElasticPolicy.standardDeviation(workerSuperStepTimes, Some(averageStepTime))
      println(s"averageStepTime: $averageStepTime, std: $standardDeviation, workers: \n")
      workerIdSuperStepTimes.foreach(println)
      if (standardDeviation.toDouble >= 0.2 * averageStepTime.toDouble) {
        println("rehashing")
        val newPartitionerBuilder = new ReHashPartitioner.Builder
        val workersHigherThanAveragePercentages = workerIdSuperStepTimes
          .filter(_._2 > averageStepTime)
          .map{
            case (workerId, averageWorkerStepTime) =>
              workerId -> ((averageWorkerStepTime - averageStepTime).toDouble / averageStepTime.toDouble)
          }.toMap

        // count #workers to balance busy workers
        // assume 1.0 = 1 worker round up
        val numWorkersNeeded = math.ceil(workersHigherThanAveragePercentages.values.sum).toInt

        // coincidentally #maxNewWorkers == #maxNumWorkers
        val maxNewWorkers = math.min((currentWorkers.keys.max + 1) + numWorkersNeeded, maxNumWorkers)
        val newWorkerIds = ((currentWorkers.keys.max + 1) until maxNewWorkers).toArray

        // repartition all according to deviation
        workersHigherThanAveragePercentages.foreach {
          case (workerId, distributionPercentage) =>
            newPartitionerBuilder.distribute(workerId, (newWorkerIds, distributionPercentage))
        }
        newPartitionerBuilder.partitioner(currentPartitioner)
        Some(Grow(newWorkerIds, newPartitionerBuilder.build()))
      } else {
        println("not rehashing")
        None
      }
    }



    decisions(currentStep) = decision
    decision
  }
}
