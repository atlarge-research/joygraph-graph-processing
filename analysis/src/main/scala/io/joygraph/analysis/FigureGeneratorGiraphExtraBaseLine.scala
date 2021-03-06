package io.joygraph.analysis

import java.util.Properties

import io.joygraph.analysis.algorithm.{AlgorithmMetric, Statistics}
import io.joygraph.analysis.external.parse.GiraphMetrics
import io.joygraph.analysis.matplotlib.VariabilityBarPerStepCramped
import org.apache.commons.math3.stat.descriptive.moment.{Mean, StandardDeviation}

import scala.collection.immutable
import scala.collection.parallel.ParIterable

object FigureGeneratorGiraphExtraBaseLine extends App {
  val propertiesConfig = {
    val prop = new Properties()
    prop.load(FigureGenerator.getClass.getResourceAsStream("/fig-generator.properties"))
    prop
  }

  val graphalyticsdir = propertiesConfig.getProperty("giraphdatagen1000tablesdir")
  val bfsMetrics = GiraphMetrics.parseFile(s"$graphalyticsdir/datagen-BFS.table")
  val wccMetrics = GiraphMetrics.parseFile(s"$graphalyticsdir/datagen-WCC.table")
  val prMetrics = GiraphMetrics.parseFile(s"$graphalyticsdir/datagen-PR.table")
  val ssspMetrics = GiraphMetrics.parseFile(s"$graphalyticsdir/datagen-SSSP.table")

  val metricsPerAlgorithm = Map(
    "BFS" -> bfsMetrics,
    "WCC" -> wccMetrics,
    "PR" -> prMetrics,
    "SSSP" -> ssspMetrics
  )

  def generateChartStatistics
  (chartFileNamePrefix : String,
   dataSet : String,
   yUnit : String,
   algorithmMetrics : Map[String, AlgorithmMetric],
   extractor : AlgorithmMetric => immutable.IndexedSeq[Iterable[(Int, Statistics)]] ) = {
    generateChart[Statistics](
      chartFileNamePrefix,
      dataSet,
      yUnit,
      algorithmMetrics,
      extractor,
      _.average
    )
  }

  def generateChartLong
  (chartFileNamePrefix : String,
   dataSet : String,
   yUnit : String,
   algorithmMetrics : Map[String, AlgorithmMetric],
   extractor : AlgorithmMetric => immutable.Seq[Iterable[(Int, Long)]] ) = {
    val mean = new Mean()
    val std = new StandardDeviation(true)

    val relevantMetrics: Map[String, Statistics] = algorithmMetrics
      .mapValues(extractor)
      .mapValues(Experiment.extractStatisticsForCramped[Long](_, _.toDouble))
      .mapValues { statistics =>
        val statisticsPerStep = statistics
        val statisticsPerResult = {
          val means = statisticsPerStep.map(_.average).toArray
          Statistics(
            std.evaluate(means),
            mean.evaluate(means),
            means.length
          )
        }

        statisticsPerResult
      }

    VariabilityBarPerStepCramped(
      relevantMetrics.keys.map('"' + _ + '"'),
      relevantMetrics.values.map(_.average),
      relevantMetrics.values.map(_.std)
    )
      .createChart(s"$chartFileNamePrefix-$dataSet", "Algorithms", s"Mean of $yUnit")
      .createCVChart(s"$chartFileNamePrefix-$dataSet", "Algorithms", s"CV of $yUnit")
  }

  def generateChart[T]
  (chartFileNamePrefix : String,
   dataSet : String,
   yUnit : String,
   algorithmMetrics : Map[String, AlgorithmMetric],
   extractor : AlgorithmMetric => immutable.IndexedSeq[Iterable[(Int, T)]],
   tToDouble : T => Double
  ) = {
    val mean = new Mean()
    val std = new StandardDeviation(true)

    val relevantMetrics: Map[String, Statistics] = algorithmMetrics
      .mapValues(extractor)
      .mapValues(Experiment.extractStatisticsForCramped[T](_, tToDouble))
      .mapValues { statistics =>
        val statisticsPerStep = statistics
        val statisticsPerResult = {
          val means = statisticsPerStep.map(_.average).toArray
          Statistics(
            std.evaluate(means),
            mean.evaluate(means),
            means.length
          )
        }

        statisticsPerResult
      }

    VariabilityBarPerStepCramped(
      relevantMetrics.keys.map('"' + _ + '"'),
      relevantMetrics.values.map(_.average),
      relevantMetrics.values.map(_.std)
    )
      .createChart(s"$chartFileNamePrefix-$dataSet", "Algorithms", s"Mean of $yUnit")
      .createCVChart(s"$chartFileNamePrefix-$dataSet", "Algorithms", s"CV of $yUnit")
  }

  generateChartLong(
    "giraph-overview-wallclock",
    "datagen-1000",
    "WallClock",
    metricsPerAlgorithm,
    _.wallClockPerStepPerWorker
  )

  generateChartLong(
    "giraph-overview-active-vertices",
    "datagen-1000",
    "Active vertices",
    metricsPerAlgorithm,
    _.activeVerticesPerStepPerWorker
  )

  generateChartStatistics(
    "giraph-overview-onheap-memory",
    "datagen-1000",
    "OnHeap-Memory",
    metricsPerAlgorithm,
    _.heapMemoryUsedPerStepPerWorker
  )

  generateChartStatistics(
    "giraph-overview-cpu-load-memory",
    "datagen-1000",
    "CPU Load",
    metricsPerAlgorithm,
    _.averageLoadPerStepPerWorker
  )

}
