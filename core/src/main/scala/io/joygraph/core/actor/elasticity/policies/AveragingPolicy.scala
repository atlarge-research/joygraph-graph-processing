package io.joygraph.core.actor.elasticity.policies

import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Result}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.ReHashPartitionerV2

import scala.collection.mutable

object AveragingPolicy {
  val STANDARD_DEVIATION_THRESHOLD = "std-threshold"
  val EPSILON : Double = 1E-4
}

abstract class AveragingPolicy[T](implicit num: Fractional[T]) extends ElasticPolicy {
  protected[this] val stepAverages = mutable.Map.empty[Int, T]
  protected[this] val vertexPartitioners = mutable.Map.empty[Int, VertexPartitioner]
  protected[this] val decisions = mutable.Map.empty[Int, Option[Result]].withDefaultValue(None)
  protected[this] val workerHistory = mutable.Map.empty[Int, Map[Int, AddressPair]]
  protected[this] var stdThreshold : Double = _

  override def init(policyParams: Config): Unit = {
    stdThreshold = policyParams.getDouble(AveragingPolicy.STANDARD_DEVIATION_THRESHOLD)
  }

  protected[this] def addVertexPartitioner(step : Int, partitioner : VertexPartitioner) : Unit = {
    vertexPartitioners(step) = partitioner
  }

  protected[this] def vertexPartitioner(step : Int) : VertexPartitioner = {
    vertexPartitioners(step)
  }

  protected[this] def addStepAverage(step : Int, average : Option[T]) : Unit = {
    println(s"stepAverage: $step $average")
    average match {
      case Some(x) =>
        stepAverages(step) = x
      case None =>
    }
  }

  protected[this] def stepAverage(step : Int) : Option[T] = stepAverages.get(step)

  protected[this] def addDecision(step : Int, decisionOpt : Option[Result]) : Unit = {
    println(s"decision: $step $decisionOpt")
    decisions(step) = decisionOpt
  }

  protected[this] def decision(step : Int) : Option[Result] = decisions(step)

  protected[this] def calculateAverage(step : Int, currentWorkers : Iterable[Int]) : Option[T]

  protected[this] def individualWorkerValues(step : Int, currentWorkers : Iterable[Int]) : Iterable[(Int,T)]

  protected[this] def previousStep(currentStep : Int) = currentStep - 1

  protected[this] def addWorkerHistory(step : Int, workers : Map[Int, AddressPair]) : Unit = {
    println(s"workerHistory: $step $workers")
    workerHistory(step) = workers
  }

  /**
    * Returns 1, if it has a positive impact given the evaluation function, negative impact -1 and there was no decision 0
    */
  protected[this] def evaluatePreviousDecision(currentStep : Int, currentWorkers : Map[Int, AddressPair]) : Int
  protected[this] def positiveEvaluationAction(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int) : Option[ElasticPolicy.Result]
  protected[this] def negativeEvaluationAction(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int) : Option[ElasticPolicy.Result]

  protected[this] def baseEvaluationAction(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int) : Option[ElasticPolicy.Result] = {
    val averageOfT = stepAverage(currentStep).get
    val workerValues = individualWorkerValues(currentStep, currentWorkers.keys)
    val standardDeviation = ElasticPolicy.standardDeviation[T](workerValues.map(_._2), Some(averageOfT))

    if (standardDeviation > stdThreshold * num.toDouble(averageOfT)) {
      val workersHigherThanAverageRatios = workerValues
        .filter{ case (workerId, tValue) =>
          num.toDouble(tValue) > num.toDouble(averageOfT)
        }
        .map{
          case (workerId, tValue) =>
            // tValue is by definition larger than the average due to the filter
            workerId -> ((num.toDouble(tValue) - num.toDouble(averageOfT)) / num.toDouble(tValue))
        }.toMap

      // count #workers to balance busy workers
      // assume 1.0 = 1 worker round up
      val numWorkersNeeded = math.ceil(workersHigherThanAverageRatios.values.sum).toInt
      val numWorkersNeededLimited = limitWorkersNeeded(currentWorkers.keys, numWorkersNeeded, maxNumWorkers)
      val newWorkerIds = generateNewWorkerIds(currentWorkers.keys, numWorkersNeededLimited)

      // if numWorkersNeeded is limited, then we need to define the ratio of the workers
      val newWorkerRatio = numWorkersNeeded.toDouble / numWorkersNeededLimited.toDouble
      val newWorkerPercentages: Iterable[WorkerPercentage] = newWorkerIds.map(workerId => WorkerPercentage(workerId, newWorkerRatio))

      val currentWorkerHigherThanAveragePercentages = workersHigherThanAverageRatios.map{
        case (workerId, percentage) => WorkerPercentage(workerId, percentage)
      }

      currentWorkerHigherThanAveragePercentages.foreach(println)

      val newPartitioner = rehashDistributions(currentPartitioner, currentWorkerHigherThanAveragePercentages, newWorkerPercentages, numWorkersNeededLimited)
      Some(Grow(newWorkerIds, newPartitioner))
    } else {
      None
    }
  }

  def rehashDistributions(prevPartitioner : VertexPartitioner, workersHigherThanAverageRatios : Iterable[WorkerPercentage], newWorkerPercentages : Iterable[WorkerPercentage], workersNeeded : Double) : VertexPartitioner = {
    val repartitionerBuilder = ReHashPartitionerV2.newBuilder()
    repartitionerBuilder.partitioner(prevPartitioner)
    workersHigherThanAverageRatios.foreach {
      case WorkerPercentage(distributorId, toBeDistributed) =>
        var distributorPercentage = toBeDistributed
        newWorkerPercentages.takeWhile(_ => distributorPercentage > AveragingPolicy.EPSILON).foreach{
          case distributee @ WorkerPercentage(distributeeId, spaceLeft) =>
            if(spaceLeft > AveragingPolicy.EPSILON) {
              val targetPercentage = math.min(spaceLeft, distributorPercentage)
              // update distributor
              distributorPercentage -= targetPercentage
              // update distributee
              distributee.percentage -= targetPercentage
              println(s"distributing: $distributorId -> $distributeeId: $targetPercentage")
              repartitionerBuilder.distribute(distributorId, (distributeeId, targetPercentage))
            }
        }
    }
    repartitionerBuilder.build()
  }

  /**
    * Default assumes workerIds are contiguous
    */
  def generateNewWorkerIds(currentWorkers : Iterable[Int], numNewWorkers : Int) : Iterable[Int] = {
    currentWorkers.size until (currentWorkers.size + numNewWorkers)
  }

  def limitWorkersNeeded(currentWorkers : Iterable[Int], workersNeeded : Int, maxWorkers : Int) : Int = {
    math.min(workersNeeded, maxWorkers - currentWorkers.size)
  }

  def enoughInformationToMakeDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Boolean

  def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int) : Option[ElasticPolicy.Result] = {
    addWorkerHistory(currentStep, currentWorkers)
    addVertexPartitioner(currentStep, currentPartitioner)
    addStepAverage(currentStep, calculateAverage(currentStep, currentWorkers.keys))

    val decision = if (enoughInformationToMakeDecision(currentStep, currentWorkers)) {
      println(s"decide: $currentStep enough information")
      val evaluation = evaluatePreviousDecision(currentStep, currentWorkers)
      println(s"evaluation: $evaluation")
      if (evaluation > 0) {
        positiveEvaluationAction(currentStep, currentWorkers, currentPartitioner, maxNumWorkers)
      } else if (evaluation < 0) {
        negativeEvaluationAction(currentStep, currentWorkers, currentPartitioner, maxNumWorkers)
      } else {
        baseEvaluationAction(currentStep, currentWorkers, currentPartitioner, maxNumWorkers)
      }
    } else {
      println(s"decide: $currentStep not enough information")
      None
    }

    addDecision(currentStep, decision)
    decision
  }

  private case class WorkerPercentage(workerId : Int, var percentage : Double)
}
