package io.joygraph.analysis.autoscale

import io.joygraph.analysis.ElasticPolicyReader
import io.joygraph.analysis.autoscale.metrics.{AccuracyMetric, InstabilityMetric, WrongProvisioningMetric}
import io.joygraph.core.actor.metrics.SupplyDemandMetrics

import scala.collection.mutable.ArrayBuffer

object AutoscalerMetricCalculator {
  def getTandUnitOfT(supplyDemands : ArrayBuffer[SupplyDemandMetrics], pairs : Iterable[(SupplyDemandMetrics, SupplyDemandMetrics)]) : (Double, Double) = {
    // discretize to timesteps
    // a unit of time is the minimum deltaT
    val minTime : Double = supplyDemands.minBy(_.timeMs).timeMs
    val maxTime : Double = supplyDemands.maxBy(_.timeMs).timeMs
    val unitOfTime : Double = pairs.map(x => x._2.timeMs - x._1.timeMs).min
    val T : Double = (maxTime - minTime) / unitOfTime
    (T, unitOfTime)
  }

  def deltaT(a : SupplyDemandMetrics, b : SupplyDemandMetrics, unitOfTime : Double) =  {
    (b.timeMs - a.timeMs) / unitOfTime
  }

  def getInstabilityMetric(supplyDemands : ArrayBuffer[SupplyDemandMetrics], rawSupplyDemands : ArrayBuffer[SupplyDemandMetrics]) : InstabilityMetric = {
    val pairs = for (i <- 1 until supplyDemands.length) yield (supplyDemands(i - 1),supplyDemands(i))

    val (t, unitOfTime) = getTandUnitOfT(supplyDemands, pairs)
    val T = t

    val sumI = pairs.map {
      case (a, b) =>
        math.min(1,
          math.max(0, math.signum(b.supply - a.supply) - math.signum(b.demand - a.demand))
        ) * deltaT(a,b, unitOfTime)
    }
    val i = (1 / (T - 1)) * sumI.sum

    val sumI2 = pairs.map {
      case (a, b) =>
        math.min(1,
          math.max(0, math.signum(b.demand - a.demand) - math.signum(b.supply - a.supply))
        ) * deltaT(a,b, unitOfTime)
    }
    val i2 = (1 / (T - 1)) * sumI2.sum

    InstabilityMetric(i, i2)
  }

  def getInstabilityMetric(policyReader: ElasticPolicyReader) : InstabilityMetric = {
    val supplyDemands = policyReader.supplyDemands
    val rawSupplyDemands = policyReader.rawSupplyDemands
    getInstabilityMetric(supplyDemands, rawSupplyDemands)
  }

  def getWrongProvisioningMetric(supplyDemands : ArrayBuffer[SupplyDemandMetrics], rawSupplyDemands : ArrayBuffer[SupplyDemandMetrics]) : WrongProvisioningMetric = {
    val pairs = for (i <- 1 until supplyDemands.length) yield (supplyDemands(i - 1),supplyDemands(i))

    val (t, unitOfTime) = getTandUnitOfT(supplyDemands, pairs)
    val T = t

    val sumU = pairs.map{
      case (a,b) =>
        Math.max(0, math.signum(b.demand - b.supply)) * deltaT(a,b, unitOfTime)
    }
    val u = (1/T) * sumU.sum

    val sumO = pairs.map{
      case (a,b) =>
        Math.max(0, math.signum(b.supply - b.demand)) * deltaT(a,b, unitOfTime)
    }

    val o = (1/T) * sumO.sum

    WrongProvisioningMetric(u, o)
  }

  def getWrongProvisioningMetric(policyReader: ElasticPolicyReader) : WrongProvisioningMetric = {
    val supplyDemands = policyReader.supplyDemands
    val rawSupplyDemands = policyReader.rawSupplyDemands
    getWrongProvisioningMetric(supplyDemands, rawSupplyDemands)
  }

  def getAccuracyMetric(supplyDemands : ArrayBuffer[SupplyDemandMetrics], rawSupplyDemands : ArrayBuffer[SupplyDemandMetrics], R : Double): AccuracyMetric = {
    val pairs = for (i <- 1 until supplyDemands.length) yield (supplyDemands(i - 1),supplyDemands(i))
    val (t, unitOfTime) = getTandUnitOfT(supplyDemands, pairs)
    val T = t
    val sumU : Double = pairs.map{
      case (a,b) =>
        Math.max(0, b.demand - b.supply) * deltaT(a,b, unitOfTime)
    }.sum
    val u = (1.0 / (T * R)) * sumU

    val sumO : Double = pairs.map{
      case (a,b) =>
        Math.max(0, b.supply - b.demand) * deltaT(a,b, unitOfTime)
    }.sum

    val o = (1.0 / (T * R)) * sumO

    val sumUN : Double = pairs.map{
      case (a,b) =>
        (Math.max(0, b.demand - b.supply) / b.demand) * deltaT(a,b, unitOfTime)
    }.sum

    val uN = (1.0 / T) * sumUN

    val sumOn : Double = pairs.map{
      case (a,b) =>
        (Math.max(0, b.supply - b.demand) / b.demand) * deltaT(a,b, unitOfTime)
    }.sum

    val oN = (1.0 / T) * sumOn

    // there are no idle resources
    val oM = 0

    AccuracyMetric(u, o, uN, oN, oM)
  }

  def getAccuracyMetric(policyReader: ElasticPolicyReader, R : Double): AccuracyMetric = {
    val supplyDemands = policyReader.supplyDemands
    val rawSupplyDemands = policyReader.rawSupplyDemands
    getAccuracyMetric(supplyDemands, rawSupplyDemands, R)
  }
}
