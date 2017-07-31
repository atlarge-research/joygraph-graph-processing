package io.joygraph.analysis

import java.util.Properties

import scala.collection.parallel.ParIterable
import scala.collection.parallel.immutable.ParMap

object FigureGeneratorExtraBaseLine extends App {
  val propertiesConfig = {
    val prop = new Properties()
    prop.load(FigureGenerator.getClass.getResourceAsStream("/fig-generator.properties"))
    prop
  }

  val graphalyticsdir = propertiesConfig.getProperty("extrabaselinedir")

  val results = ParseResultDirectories(Iterable(graphalyticsdir))
  val groupedExperiments: ParMap[String, ParIterable[Experiment]] = results.experiments.groupBy(_.dataSet)

  def buildBaseCrampedPerAlgorithm(experiments : ParIterable[Experiment], mainSb : StringBuilder) : Unit = {
    Experiment.longExtractor(
      experiments,
      _.algorithmMetrics.wallClockPerStepPerWorker,
      _.baseLineResults,
      "graphalytics-overview-wallclock",
      "WallClock"
    )

    Experiment.longExtractor(
      experiments,
      _.algorithmMetrics.activeVerticesPerStepPerWorker,
      _.baseLineResults,
      "graphalytics-overview-active-vertices",
      "Active vertices"
    )

    Experiment.statisticsExtractor(
      experiments,
      _.algorithmMetrics.offHeapMemoryPerStepPerWorker,
      _.baseLineResults,
      "graphalytics-overview-offheap-memory",
      "OffHeap-Memory"
    )

    Experiment.statisticsExtractor(
      experiments,
      _.algorithmMetrics.heapMemoryUsedPerStepPerWorker,
      _.baseLineResults,
      "graphalytics-overview-onheap-memory",
      "OnHeap-Memory"
    )

    Experiment.statisticsExtractor(
      experiments,
      _.algorithmMetrics.averageLoadPerStepPerWorker,
      _.baseLineResults,
      "graphalytics-overview-cpu-load-memory",
      "CPU Load"
    )
  }

//  groupedExperiments.foreach {
//    case (dataSet, experiments) =>
//      val mainSb = StringBuilder.newBuilder
//      buildBaseCrampedPerAlgorithm(experiments, mainSb)
//      dataSet -> mainSb.toString
//  }

  groupedExperiments.map{
    case (dataSet, experiments) =>
      dataSet -> experiments.toIndexedSeq.groupBy(_.algorithm).map {
        case (algorithm, algexperiments) =>
          algorithm -> algexperiments(0)
      }
  }.foreach {
    case (dataset, algorithmMap) => {
      algorithmMap.foreach{
        case (algorithm, experiment) => {
          experiment.createPerStepDiagrams("activeverticesperstep-%s-%s".format(algorithm, dataset))
        }
      }
    }
  }
}
