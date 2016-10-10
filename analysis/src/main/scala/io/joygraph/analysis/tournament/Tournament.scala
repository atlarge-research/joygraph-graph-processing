package io.joygraph.analysis.tournament

import io.joygraph.analysis.autoscale.metrics.{AccuracyMetric, InstabilityMetric, WrongProvisioningMetric}
import io.joygraph.analysis.{Experiment, PolicyResultProperties}

class Tournament() {

  val WIN = 1.0
  val LOSS = 0.0
  val DRAW = 0.5

  def tournament(experiment : Experiment) : Map[String, Double] = {
    val averages = experiment.policyGrouped.map {
      case (policy, results) =>
        val accuracyMetric = averageAccuracyMetric(results)
        val instabilityMetric = averageInstabilityMetric(results)
        val wrongProvisioningMetric = averageWrongProvisioningMetric(results)
        policy -> (accuracyMetric, instabilityMetric, wrongProvisioningMetric)
    }

    averages.map{
      case (p, (a,i,w)) =>
        p -> averages.map {
          case (p2, (a2, i2, w2)) =>
            fight(a, a2) + fight(i, i2) + fight (w, w2)
        }.sum
    }
  }

  def averageAccuracyMetric(results : Iterable[PolicyResultProperties]) : AccuracyMetric = {
    results.map(_.accMetric).reduce(_ += _).normalizeBy(results.size)
  }

  def averageInstabilityMetric(results : Iterable[PolicyResultProperties]) : InstabilityMetric = {
    results.map(_.instabilityMetric).reduce(_ += _).normalizeBy(results.size)
  }

  def averageWrongProvisioningMetric(results : Iterable[PolicyResultProperties]) : WrongProvisioningMetric = {
    results.map(_.wrongProvisioningMetric).reduce(_ += _).normalizeBy(results.size)
  }

  def compareLeft(a : Double, b : Double) : Double = {
    var result = 0.0
    if (a < b) {
      result += WIN
    } else if (a > b) {
      result += LOSS
    } else {
      result += DRAW
    }
    result
  }

  def compareRight(a : Double, b : Double) : Double = {
    var result = 0.0
    if (a < b) {
      result += LOSS
    } else if (a > b) {
      result += WIN
    } else {
      result += DRAW
    }
    result
  }

  def fight(a : WrongProvisioningMetric, b : WrongProvisioningMetric) : Double = {
    compareLeft(a.o, b.o) +
    compareLeft(a.u, b.u)
  }

  def fight(a : InstabilityMetric, b : InstabilityMetric) : Double = {
    compareLeft(a.i, b.i) +
    compareLeft(a.i2, b.i2)
  }

  def fight(a : AccuracyMetric, b : AccuracyMetric) : Double = {
    compareLeft(a.o, b.o) +
      compareLeft(a.u, b.u) +
      compareLeft(a.oN, b.oN) +
      compareLeft(a.uN, b.uN) +
      compareLeft(a.oM, b.oM)
  }

}
