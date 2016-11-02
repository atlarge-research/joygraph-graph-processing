package io.joygraph.analysis

import io.joygraph.analysis.autoscale.AutoscalerMetricCalculator
import io.joygraph.analysis.autoscale.metrics.{AccuracyMetric, InstabilityMetric, WrongProvisioningMetric}
import io.joygraph.analysis.figure.{DiagramFigure, ElasticTableFigure, GeneralProcessingTableFigure, TournamentScoresTableFigure}
import io.joygraph.analysis.performance.PerformanceMetric
import io.joygraph.analysis.tournament.Tournament
import io.joygraph.core.actor.metrics.{SupplyDemandMetrics, WorkerOperation}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
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

  private[this] def generatePyArray(arr : Iterable[Any]) : String = {
    "[" + arr.map(_.toString).reduce(_ + "," + _) + "]"
  }

  def createSupplyDemandPlot(outputPathPrefix : String, relativeLatexPathPrefix : String, singlePolicyPlot : Boolean = true) : Iterable[String] = {
    policyResults.sortBy(_.policyName).groupBy(_.policyName).flatMap{ case (policyName, policyResults) =>
      policyResults.zipWithIndex.filter{case (results, index) =>
        if (singlePolicyPlot) {
          index == 0
        } else {
          true
        }
      }.map{
        case (policyResult, index) =>
          val fileName = s"$dataSet-$algorithm-${policyResult.policyName}-$index.pdf"
          val outputPath = s"$outputPathPrefix/$fileName"

          val barrierTimes = policyResult.startStopTimesOf(WorkerOperation.BARRIER)
          val superStepTimes = policyResult.startStopTimesOf(WorkerOperation.RUN_SUPERSTEP)
          val barrierLabels = barrierTimes.flatMap{
            case (step, start, stop) =>
              Iterable(start -> "\"b%d\"".format(step), stop -> "\"b%d\"".format(step))
          }
          val superStepLabels = superStepTimes.flatMap{
            case (step, start, stop) =>
              Iterable(start -> "\"s%d\"".format(step), stop -> "\"s%d\"".format(step))
          }

          val demandX = policyResult.demandTimeMs().map(_._1)
          val demandY = policyResult.demandTimeMs().map(_._2)

          val supplyX = policyResult.supplyTimeMs().map(_._1)
          val supplyY = policyResult.supplyTimeMs().map(_._2)

          val supplyXPyArray = generatePyArray(supplyX)
          val supplyYPyArray = generatePyArray(supplyY)

          val demandXPyArray = generatePyArray(demandX)
          val demandYPyArray = generatePyArray(demandY)

          val xTicksBarrier = generatePyArray(barrierLabels.map(_._1))
          val xTicksBarrierLabels = generatePyArray(barrierLabels.map(_._2))

          val xTicksSuperStep = generatePyArray(superStepLabels.map(_._1))
          val xTicksSuperstepLabels = generatePyArray(superStepLabels.map(_._2))

          val script =
            s"""
               |import numpy as np
               |import matplotlib.pyplot as plt
               |
               |x1Supply = $supplyXPyArray
               |y1Supply = $supplyYPyArray
               |x2Demand = $demandXPyArray
               |y2Demand = $demandYPyArray
               |xTicksBarrier = $xTicksBarrier
               |xTicksBarrierLabels = $xTicksBarrierLabels
               |xTicksSuperStep = $xTicksSuperStep
               |xTicksSuperStepLabels = $xTicksSuperstepLabels
               |
               |minX = min(x1Supply + x2Demand + xTicksSuperStep + xTicksBarrier)
               |maxX = max(x1Supply + x2Demand + xTicksSuperStep + xTicksBarrier)
               |normMaxX = (maxX - minX) / 1000
               |
               |def subtract(x):
               |    return (x - minX) / 1000
               |
               |x1Supply = list(map(subtract, x1Supply))
               |x2Demand = list(map(subtract, x2Demand))
               |xTicksBarrier = list(map(subtract, xTicksBarrier))
               |xTicksSuperStep = list(map(subtract, xTicksSuperStep))
               |
               |xTickBarrierStart = [xTicksBarrier[i] for i in range(len(xTicksBarrier)) if i % 2 == 0]
               |xTickBarrierEnd = [xTicksBarrier[i] for i in range(len(xTicksBarrier)) if i % 2 == 1]
               |xTicksBarrierLabelsStart = [xTicksBarrierLabels[i] for i in range(len(xTicksBarrierLabels)) if i % 2 == 0]
               |xTicksBarrierLabelsEnd = [xTicksBarrierLabels[i] for i in range(len(xTicksBarrierLabels)) if i % 2 == 1]
               |xTicksSuperStep = [xTicksSuperStep[i] for i in range(len(xTicksSuperStep)) if i % 2 == 0]
               |xTicksSuperStepLabels = [xTicksSuperStepLabels[i] for i in range(len(xTicksSuperStepLabels)) if i % 2 == 0]
               |
               |fig = plt.figure()
               |barrAxes = fig.add_axes((0.1, 0.15, 0.8, 0.0))
               |supAxes = fig.add_axes((0.1, 0.3, 0.8, 0.0))
               |ax1 = fig.add_axes((0.1, 0.4, 0.8, 0.6))
               |ax2 = ax1.twinx()
               |
               |ax1.set_ylim([0.0, 21])
               |ax2.set_ylim([0.0, 21])
               |
               |ax1.set_xlim([0.0, normMaxX])
               |supAxes.set_xlim([0.0, normMaxX])
               |barrAxes.set_xlim([0.0, normMaxX])
               |barrAxes2 = barrAxes.twiny()
               |barrAxes2.set_xlim([0.0, normMaxX])
               |
               |ax1.plot(x1Supply, y1Supply)
               |ax1.set_xlabel('time (s)')
               |# Make the y-axis label and tick labels match the line color.
               |ax1.set_ylabel('supply', color='b')
               |for tl in ax1.get_yticklabels():
               |    tl.set_color('b')
               |
               |ax2.set_ylabel('demand', color='r')
               |ax2.plot(x2Demand, y2Demand, 'r')
               |
               |for tl in ax2.get_yticklabels():
               |    tl.set_color('r')
               |
               |supAxes.yaxis.set_visible(False)
               |supAxes.set_xlabel('superstep')
               |supAxes.set_xticks(xTicksSuperStep)
               |supAxes.set_xticklabels(xTicksSuperStepLabels)
               |
               |barrAxes.yaxis.set_visible(False)
               |barrAxes.set_xticks(xTickBarrierStart)
               |barrAxes.set_xticklabels(xTicksBarrierLabelsStart)
               |barrAxes.set_xlabel("barrier")
               |
               |barrAxes2.set_xticks(xTickBarrierEnd)
               |barrAxes2.set_xticklabels(xTicksBarrierLabelsEnd)
               |
               |plt.savefig("$outputPath")
      """.stripMargin

          val latexFigure =
            s"""
               |\\begin{figure}[H]
               | \\centering
               | \\includegraphics[width=0.8\\linewidth]{$relativeLatexPathPrefix/$fileName}
               | \\caption{${policyResult.policyName} on $dataSet with $algorithm}
               | \\label{policy-$dataSet-$algorithm-${policyResult.policyName}-$index.pdf}
               |\\end{figure}
        """.stripMargin

          val scriptLocation = File.makeTemp()
          scriptLocation.writeAll(script)
          scriptLocation.setExecutable(executable = true)
          new ProcessBuilder().command("/usr/bin/python", scriptLocation.toString).start().waitFor()

          latexFigure
      }
    }
  }

  def createTournamentScoreTableElastic(): String = {
    val t = new Tournament
    val scores = t.tournamentElastic(this)
    createTournamentScoreTable("elastic", "Elastic tournament scores", scores)
  }

  def createTournamentScoreTablePerformance(): String = {
    val t = new Tournament
    val scores = t.tournamentPerformance(this)
    createTournamentScoreTable("performance", "Performance tournament scores", scores)
  }

  def createTournamentScoreTableCombined(): String = {
    val t = new Tournament
    val scores = t.tournamentCombined(this)
    createTournamentScoreTable("combined", "Combined tournament scores", scores)
  }

  def createTournamentScoreTable(labelPrefix : String, partialCaption : String, scores : Map[String, Double]) : String = {
    val sortedScores = scores.toIndexedSeq.sortBy(_._2).reverse
    val builder = TournamentScoresTableFigure.newBuilder
      .algorithm(algorithm)
      .dataSet(dataSet)
      .partialCaption(partialCaption)
      .labelPrefix(labelPrefix)

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
    val meanPerformanceMetric = performanceMetrics.reduce(_ += _).normalizeBy(performanceMetrics.size)

    val numReps = performanceMetrics.length
    val processingTimes = performanceMetrics.map(_.processingTime)
    val meanProc = meanPerformanceMetric.processingTime
    val variance = processingTimes.map(_ - meanProc).map(x => x * x).sum / (numReps - 1).toDouble
    val deviation = math.sqrt(variance)
    val coefficientOfVariation = deviation / meanProc

    builder.result("Baseline", meanPerformanceMetric, coefficientOfVariation)

    policyGrouped.foreach{
      case (policyName, policyResults) =>
        val performanceMetrics = policyResults.map(_.performanceMetrics)
        val meanPerformanceMetric = performanceMetrics.reduce(_ += _).normalizeBy(performanceMetrics.size)
        val numReps = performanceMetrics.length
        val processingTimes = performanceMetrics.map(_.processingTime)
        val meanProc = meanPerformanceMetric.processingTime
        val variance = processingTimes.map(_ - meanProc).map(x => x * x).sum / (numReps - 1).toDouble
        val deviation = math.sqrt(variance)
        val coefficientOfVariation = deviation / meanProc

        builder.result(policyName, meanPerformanceMetric, coefficientOfVariation)
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
