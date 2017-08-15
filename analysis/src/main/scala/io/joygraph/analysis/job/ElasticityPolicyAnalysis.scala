package io.joygraph.analysis.job

import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Shrink}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.VertexHashPartitioner

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.io.Directory

case class ElasticityPolicyAnalysis(resultPath : String, policy : ElasticPolicy) {
  val resultPathDir = Directory(resultPath)
  val metricsPath = s"$resultPath/metrics_${resultPathDir.name}/metrics.bin"
  policy.importMetrics(metricsPath)

  val vertexHashPartitioners : mutable.Map[Int, VertexPartitioner] = mutable.Map.empty[Int, VertexPartitioner]
  val simulationResults : mutable.Map[Int, Option[ElasticPolicy.Result]] = mutable.Map.empty[Int, Option[ElasticPolicy.Result]]

  def maxSteps : Int = {
    policy.supplyDemands.map(_.superStep).max
  }

  def currentWorkersForStep(step : Int) : Map[Int, AddressPair] = {
    val supplyDemandMetrics = policy.supplyDemands.filter(x => x.superStep == (step - 1) && x.supply == x.demand)(0)
    val numWorkers = supplyDemandMetrics.demand
    (for (i <- 0 until numWorkers) yield {
      i -> AddressPair(null, null)
    }).toMap
  }

  def vertexHashPartitioner(step : Int) : Option[VertexPartitioner] = {
    vertexHashPartitioners.get(step)
  }

  @tailrec
  private def _simulate(targetStep : Int, currentStep : Int = 0) : Option[ElasticPolicy.Result] = {
    val needToCalculate = !simulationResults.isDefinedAt(currentStep)
    if (needToCalculate) {
      val currentWorkers = currentWorkersForStep(currentStep)

      val simulationResult = currentStep match {
        case 0 =>
          val partitioner = vertexHashPartitioners.getOrElseUpdate(currentStep, new VertexHashPartitioner(currentWorkers.size))
          val policyResult = policy.decide(currentStep, currentWorkers, partitioner, 20)
          policyResult match {
            case Some(result) =>
              result match {
                case Shrink(_, p) =>
                  vertexHashPartitioners(currentStep + 1) = p
                case Grow(_, p) =>
                  vertexHashPartitioners(currentStep + 1) = p
              }
            case None =>
              vertexHashPartitioners(currentStep + 1) = partitioner
          }
          policyResult
        case _ =>
          val partitioner = vertexHashPartitioners(currentStep)
          val policyResult = policy.decide(currentStep, currentWorkers, partitioner, 20)
          policyResult match {
            case Some(result) =>
              result match {
                case Shrink(_, p) =>
                  vertexHashPartitioners(currentStep + 1) = p
                case Grow(_, p) =>
                  vertexHashPartitioners(currentStep + 1) = p
              }
            case None =>
              vertexHashPartitioners.put(currentStep + 1, partitioner)
          }
          policyResult
      }
      simulationResults(currentStep) = simulationResult

      if (currentStep == targetStep) {
        simulationResult
      } else {
        _simulate(targetStep, currentStep + 1)
      }
    } else {
      _simulate(targetStep, currentStep + 1)
    }
  }

  def simulate(step : Int) : Option[ElasticPolicy.Result] = {
    if (simulationResults.isDefinedAt(step)) {
      simulationResults(step)
    } else {
      _simulate(step)
    }
  }

}
