package io.joygraph.analysis

import java.util.Properties

import io.joygraph.analysis.matplotlib.VariabilityBarPerStepCramped

import scala.collection.parallel.immutable.ParMap
import scala.collection.parallel.{ParIterable, immutable}
import scala.reflect.io.{Directory, File}

object FigureGeneratorGraphalytics extends App {
  val propertiesConfig = {
    val prop = new Properties()
    prop.load(FigureGenerator.getClass.getResourceAsStream("/fig-generator.properties"))
    prop
  }

  val graphalyticsdir = propertiesConfig.getProperty("graphalyticsjoygraphdir")


  val results = ParseResultDirectories(Iterable(graphalyticsdir))
  val groupedExperiments: ParMap[String, immutable.ParIterable[Experiment]] = results.experiments.groupBy(_.dataSet)

  def buildBaseCrampedPerAlgorithm(experiments : ParIterable[Experiment], mainSb : StringBuilder) : Unit = {
    val statisticsPerDataSetPerAlgorithm = experiments.map { x =>
      x.dataSet -> x.createCrampedWallClock()
    }.toIndexedSeq
      .groupBy(_._1).map{
      case (dataSet, algorithmsMap) =>
        dataSet -> algorithmsMap.map(_._2).reduce(_ ++ _)
    }

    statisticsPerDataSetPerAlgorithm.foreach{
      case (dataSet, statisticsPerAlgorithm) =>
        VariabilityBarPerStepCramped(
          statisticsPerAlgorithm.keys.map('"' + _ + '"'),
          statisticsPerAlgorithm.values.map(_.average),
          statisticsPerAlgorithm.values.map(_.std)
        ).createChart(s"overview-wallclock-$dataSet", "Algorithms", "Wallclock")
    }

    // TODO make this work
    experiments.map { x =>
      x.dataSet -> Experiment.createCrampedStatistics(
        x.baseLineResults,
        _.algorithmMetrics.offHeapMemoryPerStepPerWorker
      )
    }.toIndexedSeq
      .groupBy(_._1).map{
      case (dataSet, algorithmsMap) =>
        dataSet -> algorithmsMap.map(_._2).reduce(_ ++ _)
    }.foreach{
      case (dataSet, statisticsPerAlgorithm) =>
        VariabilityBarPerStepCramped(
          statisticsPerAlgorithm.keys.map('"' + _ + '"'),
          statisticsPerAlgorithm.values.map(_.average),
          statisticsPerAlgorithm.values.map(_.std)
        ).createChart(s"overview-offheap-memory-$dataSet", "Algorithms", "Wallclock")
    }

    // TODO Add not only wallclock but also graphalytics

  }

  groupedExperiments.foreach {
    case (dataSet, experiments) =>
      val mainSb = StringBuilder.newBuilder
      buildBaseCrampedPerAlgorithm(experiments, mainSb)
      dataSet -> mainSb.toString
  }
}
