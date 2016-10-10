package io.joygraph.analysis

import io.joygraph.analysis.autoscale.AutoscalerMetricCalculator
import io.joygraph.analysis.autoscale.metrics.{AccuracyMetric, InstabilityMetric, WrongProvisioningMetric}
import io.joygraph.analysis.figure.{DiagramFigure, ElasticTableFigure, GeneralProcessingTableFigure, TournamentScoresTableFigure}
import io.joygraph.analysis.performance.PerformanceMetric
import io.joygraph.analysis.tournament.Tournament
import io.joygraph.core.actor.metrics.SupplyDemandMetrics

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

case class Experiment(dataSet : String, algorithm : String, experimentalResults : Iterable[ExperimentalResult]) {

  val baseLineResults = ArrayBuffer.empty[GeneralResultProperties]
  val policyResults = ArrayBuffer.empty[PolicyResultProperties]
  val invalidResults = ArrayBuffer.empty[BaseResultProperties]
  lazy val policyGrouped = policyResults.groupBy(_.policyName)

  experimentalResults.foreach { r =>
    Try[PolicyResultProperties] {
      new ExperimentalResult(r.dir) with PolicyResultProperties
    } match {
      case Failure(exception) =>
        Try[GeneralResultProperties] {
          new ExperimentalResult(r.dir) with GeneralResultProperties
        } match {
          case Failure(exception) =>
            println(exception)
            invalidResults += r
          case Success(value) =>
            baseLineResults += value
        }
      case Success(value) =>
        policyResults += value
    }
  }

  def createTournamentScoreTable(): String = {
    val t = new Tournament
    val scores = t.tournament(this)
    val sortedScores = scores.toIndexedSeq.sortBy(_._2).reverse
    val builder = TournamentScoresTableFigure.newBuilder
      .algorithm(algorithm)
      .dataSet(dataSet)

    sortedScores.foreach{
      case (policy, score) =>
        builder.result(policy, score)
    }
    builder.build()
  }

  def createElasticTableWithAverages(): String = {
    val builder = ElasticTableFigure.newBuilder
      .algorithm(algorithm)
      .dataSet(dataSet)

    val derivedMetricsFromBaseline = baseLineResults.map{ baseLineResult =>
      val totalNumberOfSteps = baseLineResult.metrics.policyMetricsReader.totalNumberOfSteps()
      val numWorkers = baseLineResult.metrics.policyMetricsReader.workersForStep(0).size
      val derivedSupplyDemand = mutable.ArrayBuffer.empty[SupplyDemandMetrics]
      (0 until totalNumberOfSteps).foreach{
        step =>
          derivedSupplyDemand += SupplyDemandMetrics(step, step, numWorkers, numWorkers)
      }
      (AutoscalerMetricCalculator.getAccuracyMetric(derivedSupplyDemand, derivedSupplyDemand, numWorkers),
        AutoscalerMetricCalculator.getWrongProvisioningMetric(derivedSupplyDemand, derivedSupplyDemand),
          AutoscalerMetricCalculator.getInstabilityMetric(derivedSupplyDemand, derivedSupplyDemand)
        )
    }

    val (baseAccuracyMetricsSum, baseWrongProvisioningSum, baseInstabilitySum) = derivedMetricsFromBaseline.reduce[(AccuracyMetric, WrongProvisioningMetric, InstabilityMetric)]{
      case ((a, b, c), (a2, b2, c2)) =>
        (a += a2, b += b2, c += c2)
    }

    val baseResultsSize = baseLineResults.size

    builder.policyResult("Baseline",
      baseAccuracyMetricsSum.normalizeBy(baseResultsSize),
      baseWrongProvisioningSum.normalizeBy(baseResultsSize),
      baseInstabilitySum.normalizeBy(baseResultsSize)
    )

    policyGrouped.foreach{
      case (policyName, policyResults) =>
        val accuracyMetrics = policyResults.map(_.accMetric)
        val instabilityMetrics = policyResults.map(_.instabilityMetric)
        val wrongProvisioningMetrics = policyResults.map(_.wrongProvisioningMetric)
        val averageAccuracyMetric = accuracyMetrics.reduce(_ += _).normalizeBy(accuracyMetrics.size)
        val averageInstabilityMetric = instabilityMetrics.reduce(_ += _).normalizeBy(instabilityMetrics.size)
        val averageWrongProvisioningMetric = wrongProvisioningMetrics.reduce(_ += _).normalizeBy(wrongProvisioningMetrics.size)
        builder.policyResult(policyName, averageAccuracyMetric, averageWrongProvisioningMetric, averageInstabilityMetric)
    }
    builder.build()
  }

  def createPerformanceTableWithAverages(): String = {
    val builder = GeneralProcessingTableFigure.newBuilder
      .algorithm(algorithm)
      .dataSet(dataSet)

    val performanceMetrics = baseLineResults.map(_.performanceMetrics)
    val averagePerformanceMetric = performanceMetrics.reduce(_ += _).normalizeBy(performanceMetrics.size)
    builder.result("Baseline", averagePerformanceMetric)

    policyGrouped.foreach{
      case (policyName, policyResults) =>
        val performanceMetrics = policyResults.map(_.performanceMetrics)
        val averagePerformanceMetric = performanceMetrics.reduce(_ += _).normalizeBy(performanceMetrics.size)
        builder.result(policyName, averagePerformanceMetric)
    }
    builder.build()
  }

  def createLinePlotFigure(extractor : ElasticPolicyReader => Iterable[Double], xAxisLabel : String, yAxisLabel : String, diagramTitle : String, fileName : String) : String = {
    val builder =
      DiagramFigure.newBuilder
        .fileName(fileName)
        .xAxisLabel(xAxisLabel)
        .yAxisLabel(yAxisLabel)
        .diagramTitle(diagramTitle)

    val xValues = baseLineResults.map(_.metrics.policyMetricsReader)
    ???
  }

  def createFigureVerticesPerSecondFigure(extractor : PerformanceMetric => Long, xAxisLabel : String, yAxisLabel : String, diagramTitle : String, fileName : String) : String = {
    val builder =
      DiagramFigure.labelOnlyNewBuilder
      .fileName(fileName)
      .xAxisLabel(xAxisLabel)
      .yAxisLabel(yAxisLabel)
      .diagramTitle(diagramTitle)

    val performanceMetrics = baseLineResults.map(_.performanceMetrics)
    val averagePerformanceMetric = performanceMetrics.reduce(_ += _).normalizeBy(performanceMetrics.size)
    val (min, max) = performanceMetrics.map( x => (x,x)).reduce[(PerformanceMetric, PerformanceMetric)] {
      case ((x1, x2), (y1, y2)) =>
        (x1.min(y1), x2.max(y2))
    }
    val minErr = min.diff(averagePerformanceMetric)
    val maxErr = max.diff(averagePerformanceMetric)

    builder.values(
      ("Baseline",
        extractor(averagePerformanceMetric),
        (extractor(minErr), extractor(maxErr)))
    )

    policyGrouped.foreach {
      case (policyName, policyResults) =>
        val performanceMetrics = policyResults.map(_.performanceMetrics)
        val averagePerformanceMetric = performanceMetrics.reduce(_ += _).normalizeBy(performanceMetrics.size)
        val (min, max) = performanceMetrics.map( x => (x,x)).reduce[(PerformanceMetric, PerformanceMetric)] {
          case ((x1, x2), (y1, y2)) =>
            (x1.min(y1), x2.max(y2))
        }
        val minErr = min.diff(averagePerformanceMetric)
        val maxErr = max.diff(averagePerformanceMetric)

        builder.values(
          (policyName,
            extractor(averagePerformanceMetric),
            (extractor(minErr), extractor(maxErr)))
        )
    }
    builder.build()
  }
}
