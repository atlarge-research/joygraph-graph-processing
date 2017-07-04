package io.joygraph.analysis

import io.joygraph.analysis.algorithm.Statistics
import io.joygraph.analysis.autoscale.AutoscalerMetricCalculator
import io.joygraph.analysis.autoscale.metrics.{AccuracyMetric, InstabilityMetric, WrongProvisioningMetric}
import io.joygraph.analysis.figure._
import io.joygraph.analysis.performance.PerformanceMetric
import io.joygraph.analysis.tournament.Tournament
import io.joygraph.core.actor.metrics.{SupplyDemandMetrics, WorkerOperation}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
import scala.util.{Failure, Success, Try}

case class Experiment(dataSet : String, algorithm : String, experimentalResults : Iterable[ExperimentalResult]) {

  val baseLineResults: ArrayBuffer[GeneralResultProperties] = ArrayBuffer.empty[GeneralResultProperties]
  val policyResults: ArrayBuffer[PolicyResultProperties] = ArrayBuffer.empty[PolicyResultProperties]
  val invalidResults: ArrayBuffer[BaseResultProperties] = ArrayBuffer.empty[BaseResultProperties]
  lazy val policyGrouped: Map[String, ArrayBuffer[PolicyResultProperties]] = policyResults.groupBy(_.policyName)

  experimentalResults.foreach { r =>
    Try[PolicyResultProperties] {
      new ExperimentalResult(r.dir) with PolicyResultProperties
    } match {
      case Failure(_) =>
        Try[GeneralResultProperties] {
          new ExperimentalResult(r.dir) with GeneralResultProperties
        } match {
          case Failure(invalidError) =>
            println(r.benchmarkId + " " + invalidError)
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

  def plotSupplyDemands(policyName : String, policyResults : Iterable[PolicyResultProperties], outputPathPrefix : String, relativeLatexPathPrefix : String, latexOnly : Boolean) : String = {
    val numMinipages = policyResults.size.toDouble
    val pageFraction = 1.0 / numMinipages - 0.01

    val latexFigures = policyResults.zipWithIndex.map {
      case (policyResult, index) =>
        val fileName = s"$dataSet-$algorithm-${policyResult.policyName}-$index.pdf"

        if (!latexOnly) {
          val outputPath = s"$outputPathPrefix/$fileName"

          val barrierTimes = policyResult.startStopTimesOf(WorkerOperation.BARRIER)
          val superStepTimes = policyResult.startStopTimesOf(WorkerOperation.RUN_SUPERSTEP)
          val barrierLabels = barrierTimes.flatMap{
            case (step, Some((start, stop))) =>
              Iterable(start -> "\"b%d\"".format(step), stop -> "\"b%d\"".format(step))
            case (step, None) =>
              Iterable()
          }
          val superStepLabels = superStepTimes.flatMap{
            case (step, Some((start, stop))) =>
              Iterable(start -> "\"%d\"".format(step), stop -> "\"%d\"".format(step))
            case (step, None) =>
              Iterable()
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
               |import matplotlib.pyplot as plt
               |
               |x1Supply = $supplyXPyArray
               |y1Supply = $supplyYPyArray
               |x2Demand = $demandXPyArray
               |y2Demand = $demandYPyArray
               |xTicksSuperStep = $xTicksSuperStep
               |xTicksSuperStepLabels = $xTicksSuperstepLabels
               |
               |minX = min(x1Supply + x2Demand + xTicksSuperStep)
               |maxX = max(x1Supply + x2Demand + xTicksSuperStep)
               |normMaxX = (maxX - minX) / 1000
               |
               |def subtract(x):
               |    return (x - minX) / 1000
               |
               |x1Supply = list(map(subtract, x1Supply))
               |x2Demand = list(map(subtract, x2Demand))
               |xTicksSuperStep = list(map(subtract, xTicksSuperStep))
               |
               |xTicksSuperStep = [xTicksSuperStep[i] for i in range(len(xTicksSuperStep)) if i % 2 == 0]
               |xTicksSuperStepLabels = [xTicksSuperStepLabels[i] for i in range(len(xTicksSuperStepLabels)) if i % 2 == 0]
               |
               |fig = plt.figure()
               |supAxes = fig.add_axes((0.1, 0.1, 0.8, 0.0))
               |ax1 = fig.add_axes((0.1, 0.2, 0.8, 0.8))
               |ax2 = ax1.twinx()
               |
               |ax1.set_ylim([0.0, 21])
               |ax2.set_ylim([0.0, 21])
               |
               |ax1.set_xlim([0.0, normMaxX])
               |supAxes.set_xlim([0.0, normMaxX])
               |
               |p1 = ax1.plot(x1Supply, y1Supply)
               |p2 = ax2.plot(x2Demand, y2Demand, 'r')
               |
               |ax1.set_xlabel('time (s)')
               |# Make the y-axis label and tick labels match the line color.
               |ax1.set_ylabel('machines')
               |ax1.legend((p1[0], p2[0]), ('supply', 'demand'), loc = 0)
               |
               |supAxes.yaxis.set_visible(False)
               |supAxes.set_xlabel('superstep')
               |supAxes.set_xticks(xTicksSuperStep)
               |supAxes.set_xticklabels(xTicksSuperStepLabels)
               |plt.savefig("$outputPath")
              """.stripMargin

          val scriptLocation = File.makeTemp()
          scriptLocation.writeAll(script)
          scriptLocation.setExecutable(executable = true)
          new ProcessBuilder().command("/usr/bin/python", scriptLocation.toString).start().waitFor()
        }

        val latexFigure =
          s"""|\\begin{minipage}{${"%.2f".format(pageFraction)}\\linewidth}
              | \\centering
              | \\includegraphics[width=1.0\\linewidth]{$relativeLatexPathPrefix/$fileName}
              |\\end{minipage}""".stripMargin

        latexFigure
    }

    s"""
       |\\begin{figure}[H]
       |${latexFigures.reduce(_ + "" + _)}
       |\\caption{Supply and demand plot for $policyName on $dataSet with ${algorithm.toUpperCase}.}
       |\\label{policy-$dataSet-$algorithm-$policyName-supply-demand}
       |\\end{figure}
     """.stripMargin
  }

  def plotBarVariability
  (policyName: String,
   groupedResults: ArrayBuffer[PolicyResultProperties],
   outputPathPrefix: String,
   relativeLatexPathPrefix: String, latexOnly : Boolean): String = {
    val fileName = s"variability-$dataSet-$algorithm-$policyName.pdf"
    if (!latexOnly) {
      val numResults = groupedResults.size
      val data: mutable.Seq[(Int, Iterable[(Int, Long)], Iterable[(Int, Long)])] = groupedResults.map{ policyResult =>
        val superstepTimes: Iterable[(Int, Long)] = policyResult.startStopTimesOf(WorkerOperation.RUN_SUPERSTEP).map{
          case (step, Some((start, stop))) =>
            step -> (stop - start)
          case (step, _) =>
            step -> 0L
        }

        val elasticOverheadTimes: Iterable[(Int, Long)] = policyResult.startStopTimesOf(WorkerOperation.DISTRIBUTE_DATA).map{
          case (step, Some((start, stop))) =>
            step -> (stop - start)
          case (step, _) =>
            step -> 0L
        }
        val numberOfSteps = policyResult.metrics.policyMetricsReader.totalNumberOfSteps()
        (numberOfSteps, superstepTimes, elasticOverheadTimes)
      }

      val (numberOfSteps, sumStepTimes, sumElasticOverheadTimes) = data.reduce[(Int, Iterable[(Int, Long)], Iterable[(Int, Long)])] {
        case (a, b) =>
          val (numSteps, superStepTimes, elasticOverheadTimes) = a
          val (_, superStepTimes2, elasticOverheadTimes2) = b

          val sumStepTimes: Iterable[(Int, Long)] = (superStepTimes, superStepTimes2).zipped.map {
            case ((step, v), (_, v2)) =>
              (step, v + v2)
          }

          val sumElasticOverheadTimes: Iterable[(Int, Long)] = (elasticOverheadTimes, elasticOverheadTimes2).zipped.map {
            case ((step, v), (_, v2)) =>
              (step, v + v2)
          }

          (numSteps, sumStepTimes, sumElasticOverheadTimes)
      }
      val meansSuperStepTimes: Iterable[(Int, Double)] = sumStepTimes.map {
        case (step, sumStepTime) =>
          step -> (sumStepTime.toDouble / numResults)
      }

      val averageSumElasticOverheadTimes = sumElasticOverheadTimes.map {
        case (step, sumElasticOverheadTime) =>
          step -> (sumElasticOverheadTime.toDouble / numResults)
      }

      val meansSuperStepTimesMap: Map[Int, Double] = meansSuperStepTimes.toMap
      val meansElasticTimesMap: Map[Int, Double] = averageSumElasticOverheadTimes.toMap

      data.map(_._2).map(_.toMap)

      val (stdsSum, stdsElasticSum) = data.map { x =>
        val (_, superStepTimes, elasticOverheadTimes) = x

        val stds = superStepTimes.map{
          case (step, superStepTime) =>
            val diff = superStepTime - meansSuperStepTimesMap(step)
            step -> (diff * diff)
        }

        val stdselastic = elasticOverheadTimes.map {
          case (step, superStepTime) =>
            val diff = superStepTime - meansElasticTimesMap(step)
            step -> (diff * diff)
        }
        (stds, stdselastic)
        //      (0, Math.sqrt((1/ (stds.size - 1)) * stds.sum), Math.sqrt((1/ (stdselastic.size - 1)) * stdselastic.sum))
      }.reduce[(Iterable[(Int, Double)], Iterable[(Int, Double)])] {
        case (a, b) =>
          val (stds, stdselastic) = a
          val (stds2, stdselastic2) = b
          val stdsPartSum = (stds, stds2).zipped.map{
            case ((step, v), (_, v2)) =>
              step -> (v + v2)
          }
          val stdsElasticPartSum = (stdselastic, stdselastic2).zipped.map {
            case ((step, v), (_, v2)) =>
              step -> (v + v2)
          }

          (stdsPartSum, stdsElasticPartSum)
      }

      val stdsSuperStepTimes = stdsSum.map{
        case (step, stdSum) =>
          step -> Math.sqrt(1.0 / (numResults.toDouble - 1.0) * stdSum.toDouble)
      }

      val stdsElasticSuperStepTimes = stdsElasticSum.map{
        case (step, stdElasticSum) =>
          step -> Math.sqrt(1.0 / (numResults.toDouble - 1.0) * stdElasticSum.toDouble)
      }

      val procSpeedErrors = stdsSuperStepTimes.map{
        case (step, stdProc) =>
          step -> stdProc
      }

      val elasticOverheadErrors = stdsElasticSuperStepTimes.map{
        case (step, stdProcElastic) =>
          step -> stdProcElastic
      }

      val superStepTimes = generatePyArray(meansSuperStepTimes.map(_._2))
      val elasticOverheadTimesPyArray = generatePyArray(averageSumElasticOverheadTimes.map(_._2))
      val procSpeedErrorsPyArray = generatePyArray(procSpeedErrors.map(_._2))
      val elasticOverheadErrorsPyArray = generatePyArray(elasticOverheadErrors.map(_._2))

      val outputPath = s"$outputPathPrefix/$fileName"

      val script =
        s"""
           |import numpy as np
           |import matplotlib.pyplot as plt
           |
           |numSupersteps = $numberOfSteps
           |yAverageProcSpeed = $superStepTimes
           |yElasticOverhead = $elasticOverheadTimesPyArray
           |yProcSpeedError = $procSpeedErrorsPyArray
           |yElasticOverheadError = $elasticOverheadErrorsPyArray
           |
           |yProcSpeedError = list(map(lambda x: x / 1000, yProcSpeedError))
           |yElasticOverheadError = list(map(lambda x: x / 1000, yElasticOverheadError))
           |yAverageProcSpeed = list(map(lambda x: x / 1000, yAverageProcSpeed))
           |yElasticOverhead = list(map(lambda x: x / 1000, yElasticOverhead))
           |
           |barWidth = 0.35
           |steps = np.arange(0, numSupersteps, 1)
           |
           |fig = plt.figure()
           |barChart = fig.add_axes((0.1, 0.1, 0.8, 0.8))
           |barChart.set_xlim([-0.1, max(steps) + 0.5])
           |p1 = barChart.bar(steps, yAverageProcSpeed, barWidth, color='r', yerr = yProcSpeedError)
           |p2 = barChart.bar(steps, yElasticOverhead, barWidth, bottom=yAverageProcSpeed, yerr = yElasticOverheadError)
           |barChart.set_xticks(steps + barWidth/2.)
           |barChart.set_xticklabels(steps)
           |barChart.set_xlabel('supersteps')
           |barChart.set_ylabel('time (s)')
           |barChart.legend((p1[0], p2[0]), ('t_proc', 't_elastic'), loc = 0)
           |plt.savefig("$outputPath")
       """.stripMargin

      val scriptLocation = File.makeTemp()
      scriptLocation.writeAll(script)
      scriptLocation.setExecutable(executable = true)
      new ProcessBuilder().command("/usr/bin/python", scriptLocation.toString).start().waitFor()
    }

    s"""
       |\\begin{figure}[H]
       | \\centering
       | \\includegraphics[width=1.0\\linewidth]{$relativeLatexPathPrefix/$fileName}
       |\\caption{Variability of $policyName on $dataSet with ${algorithm.toUpperCase}.}
       |\\label{policy-$dataSet-$algorithm-$policyName-variability}
       |\\end{figure}
     """.stripMargin
  }

  def plotHighDetail
  (policyName: String,
   groupedResults: ArrayBuffer[PolicyResultProperties],
   outputPathPrefix: String,
   relativeLatexPathPrefix: String, latexOnly : Boolean): String = {
    groupedResults.map{
      result =>
        // TODO
    }
    ???
  }

  def createSupplyDemandPlot(outputPathPrefix : String, relativeLatexPathPrefix : String, latexOnly : Boolean) : Iterable[String] = {
    policyResults.sortBy(_.policyName).groupBy(_.policyName).map{ case (policyName, groupedResults) =>
      val sortByExperimentDate = groupedResults.sortBy(_.experimentDate).takeRight(3)
      plotBarVariability(policyName, sortByExperimentDate, outputPathPrefix, relativeLatexPathPrefix, latexOnly) +
      plotSupplyDemands(policyName, sortByExperimentDate, outputPathPrefix, relativeLatexPathPrefix, latexOnly)
    }
  }

  def createTournamentScoreTable(labelPrefix : String, partialCaption : String, elasticScores: Map[String, Double], performanceScores: Map[String, Double], combinedScores: Map[String, Double]): String = {
    val sortedElasticScores = elasticScores.toIndexedSeq.sortBy(_._2).reverse
    val sortedPerformanceScores = performanceScores.toIndexedSeq.sortBy(_._2).reverse
    val sortedCombinedScores = combinedScores.toIndexedSeq.sortBy(_._2).reverse
    val builder = MergedTournamentScoresTableFigure.newBuilder
      .algorithm(algorithm)
      .dataSet(dataSet)
      .partialCaption(partialCaption)
      .labelPrefix(labelPrefix)

    (sortedElasticScores, sortedPerformanceScores, sortedCombinedScores).zipped.foreach {
      case (pair, pair2, pair3) =>
        builder.result((pair, pair2, pair3))
    }

    builder.build()
  }

  def createTournamentScoreTableMerged() : String = {
    val t = new Tournament
    val elasticScores = t.tournamentElastic(this)
    val performanceScores = t.tournamentPerformance(this)
    val combinedScores = t.tournamentCombined(this)

    createTournamentScoreTable("merged", "Elastic, Performance and Combined tournament scores", elasticScores, performanceScores, combinedScores)
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
      case (policyName, results) =>
        val accuracyMetrics = results.map(_.accMetric)
        val instabilityMetrics = results.map(_.instabilityMetric)
        val wrongProvisioningMetrics = results.map(_.wrongProvisioningMetric)
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
      case (policyName, results) =>
        val performanceMetrics = results.map(_.performanceMetrics)
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
      case (policyName, results) =>
        val performanceMetrics = results.map(_.performanceMetrics)
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

  def createVerticesPerStepDiagrams(fileName : String) : String = {
    this.policyGrouped.foreach {
      case (policyName, results) =>
        results.zipWithIndex.foreach {
          case (result, index) =>
            val avpw = result.algorithmMetrics.activeVerticesPerStepPerWorker
            val multiDiagramFigure = MultiDiagramFigure.builder
            multiDiagramFigure.fileName(s"$fileName-$policyName-$index")
              .diagramTitle(s"$fileName-$policyName-$index")
            avpw.zipWithIndex.foreach{
              case (v, step) =>
                val builder = MultiDiagramFigure.diagramBuilder
                val vMap: Map[Int, Long] = v.toMap
                builder.yAxisLabel(s"Step $step")
                builder.xAxisLabel(s"Num workers ${vMap.size}")
                for (workerId <- 0 until result.maxWorkerCount) {
                  val numVertices : String = vMap.get(workerId) match {
                    case Some(verticesCount) => verticesCount.toString
                    case None => "None"
                  }
                  builder.values(
                    (workerId.toInt, numVertices, (0,0))
                  )
                }
                multiDiagramFigure.addSubPlot(builder)
            }
            multiDiagramFigure.build()
        }
    }
    ""
  }

  def createBytesSentPerStepDiagrams(fileName : String) : String = {
    this.policyGrouped.foreach {
      case (policyName, results) =>
        results.zipWithIndex.foreach {
          case (result, index) =>
            val avpw = result.algorithmMetrics.bytesSentPerStepPerWorker
            val multiDiagramFigure = MultiDiagramFigure.builder
            multiDiagramFigure.fileName(s"$fileName-$policyName-$index")
              .diagramTitle(s"$fileName-$policyName-$index")
            avpw.zipWithIndex.foreach{
              case (v, step) =>
                val builder = MultiDiagramFigure.diagramBuilder
                val vMap: Map[Int, Statistics] = v.toMap
                builder.yAxisLabel(s"Step $step")
                builder.xAxisLabel(s"Num workers ${vMap.size}")
                for (workerId <- 0 until result.maxWorkerCount) {
                  val numVertices : String = vMap.get(workerId) match {
                    case Some(statistics) => (statistics.average * statistics.n).toString
                    case None => "None"
                  }
                  builder.values(
                    (workerId.toInt, numVertices, (0,0))
                  )
                }
                multiDiagramFigure.addSubPlot(builder)
            }
            multiDiagramFigure.build()
        }
    }
    ""
  }

  def createBytesReceivedPerStepDiagrams(fileName : String) : String = {
    this.policyGrouped.foreach {
      case (policyName, results) =>
        results.zipWithIndex.foreach {
          case (result, index) =>
            val avpw = result.algorithmMetrics.bytesReceivedPerStepPerWorker
            val multiDiagramFigure = MultiDiagramFigure.builder
            multiDiagramFigure.fileName(s"$fileName-$policyName-$index")
              .diagramTitle(s"$fileName-$policyName-$index")
            avpw.zipWithIndex.foreach{
              case (v, step) =>
                val builder = MultiDiagramFigure.diagramBuilder
                val vMap: Map[Int, Statistics] = v.toMap
                builder.yAxisLabel(s"Step $step")
                builder.xAxisLabel(s"Num workers ${vMap.size}")
                for (workerId <- 0 until result.maxWorkerCount) {
                  val numVertices : String = vMap.get(workerId) match {
                    case Some(statistics) => (statistics.average * statistics.n).toString
                    case None => "None"
                  }
                  builder.values(
                    (workerId.toInt, numVertices, (0,0))
                  )
                }
                multiDiagramFigure.addSubPlot(builder)
            }
            multiDiagramFigure.build()
        }
    }
    ""
  }

  def createOffHeapMemoryPerStepDiagrams(fileName : String) : String = {
    this.policyGrouped.foreach {
      case (policyName, results) =>
        results.zipWithIndex.foreach {
          case (result, index) =>
            val avpw = result.algorithmMetrics.offHeapMemoryPerStepPerWorker
            val multiDiagramFigure = MultiDiagramFigure.builder
            multiDiagramFigure.fileName(s"$fileName-$policyName-$index")
              .diagramTitle(s"$fileName-$policyName-$index")
            avpw.zipWithIndex.foreach{
              case (v, step) =>
                val builder = MultiDiagramFigure.diagramBuilder
                val vMap: Map[Int, Statistics] = v.toMap
                builder.yAxisLabel(s"Step $step")
                builder.xAxisLabel(s"Num workers ${vMap.size}")
                for (workerId <- 0 until result.maxWorkerCount) {
                  val numVertices : String = vMap.get(workerId) match {
                    case Some(statistics) => (statistics.average * statistics.n).toString
                    case None => "None"
                  }
                  builder.values(
                    (workerId.toInt, numVertices, (0,0))
                  )
                }
                multiDiagramFigure.addSubPlot(builder)
            }
            multiDiagramFigure.build()
        }
    }
    ""
  }

  def createWallClockPerStepDiagrams(fileName : String) : String = {
    this.policyGrouped.foreach {
      case (policyName, results) =>
        results.zipWithIndex.foreach {
          case (result, index) =>
            val avpw = result.algorithmMetrics.wallClockPerStepPerWorker
            val multiDiagramFigure = MultiDiagramFigure.builder
            multiDiagramFigure.fileName(s"$fileName-$policyName-$index")
              .diagramTitle(s"$fileName-$policyName-$index")
            avpw.zipWithIndex.foreach{
              case (v, step) =>
                val builder = MultiDiagramFigure.diagramBuilder
                val vMap: Map[Int, Long] = v.toMap
                builder.yAxisLabel(s"Step $step")
                builder.xAxisLabel(s"Num workers ${vMap.size}")
                for (workerId <- 0 until result.maxWorkerCount) {
                  val numVertices : String = vMap.get(workerId) match {
                    case Some(verticesCount) => verticesCount.toString
                    case None => "None"
                  }
                  builder.values(
                    (workerId.toInt, numVertices, (0,0))
                  )
                }
                multiDiagramFigure.addSubPlot(builder)
            }
            multiDiagramFigure.build()
        }
    }
    ""
  }

  def createPerStepDiagrams(fileName : String): String = {
    val ((datasetName, algorithmName), properties) = this.baseLineResults.groupBy(x => x.datasetName -> x.algorithmName).mapValues {
      _.collectFirst {
        case x : GeneralResultProperties => x
      }.get
    }.iterator.next()
    val metrics = properties.algorithmMetrics
    val activeVerticesPerStep = metrics.activeVerticesPerStep.zipWithIndex

    val builder =
      DiagramFigure.labelOnlyNewBuilder
        .fileName(fileName)
        .sortByY(false)
        .xAxisLabel("Steps")
        .yAxisLabel("Number of active vertices")
        .manualXAxis(
          0.toString,
          activeVerticesPerStep.maxBy(_._2)._2.toString
        )
        .logScale(true)
//        .diagramTitle("Active vertices per step for %s on %s".format(datasetName, algorithmName))

    activeVerticesPerStep.foreach{
      case (activeVertices, step) => {
        builder.values(
          (step.toString, activeVertices, (0, 0))
        )
      }
    }

    builder.build()

  }

}
