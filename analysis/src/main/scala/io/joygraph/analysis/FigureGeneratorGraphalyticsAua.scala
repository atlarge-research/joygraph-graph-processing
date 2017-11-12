package io.joygraph.analysis

import java.util.Properties

import io.joygraph.analysis.FigureGenerator.{LATEXONLY, relativeFigPathDir}

import scala.collection.parallel.ParIterable
import scala.collection.parallel.immutable.ParMap

object FigureGeneratorGraphalyticsAua extends App {
  val propertiesConfig = {
    val prop = new Properties()
    prop.load(FigureGenerator.getClass.getResourceAsStream("/fig-generator.properties"))
    prop
  }

  val graphalyticsdir = propertiesConfig.getProperty("auadir")

  val results = ParseResultDirectories(Iterable(graphalyticsdir))
  val groupedExperiments: Map[String, Iterable[Experiment]] = results.experiments.toIndexedSeq.groupBy(_.dataSet)

  def buildBaseCrampedPerAlgorithm(experiments : Iterable[Experiment], mainSb : StringBuilder) : Unit = {
    experiments.map { x =>
      val sb = StringBuilder.newBuilder
      //      sb.append(x.createVerticesPerStepDiagrams("activeverticesperstepperworker-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      //      sb.append(x.createWallClockPerStepDiagrams("wallclock-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      //      sb.append(x.createBytesSentPerStepDiagrams("bytesSent-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      //      sb.append(x.createBytesReceivedPerStepDiagrams("bytesReceived-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      //      sb.append(x.createOffHeapMemoryPerStepDiagrams("offHeap-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      //      sb.append(x.createAverageCPUPerStepDiagrams("avgCPU-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      sb.append(x.createVerticesPerStepBarDiagrams("activeverticesperstepperworker-bar-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      sb.append(x.createWallClockPerStepBarDiagrams("wallclock-bar-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      sb.append(x.createBytesSentPerStepBarDiagrams("bytesSent-bar-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      sb.append(x.createBytesReceivedPerStepBarDiagrams("bytesReceived-bar-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      sb.append(x.createOffHeapMemoryPerStepBarDiagrams("offHeap-bar-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      sb.append(x.createAverageCPUPerStepBarDiagrams("avgCPU-bar-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
//      sb.append(x.createPerStepDiagrams("activeverticesperstep-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      x.createSupplyDemandPlot(System.getProperty("user.dir"), relativeLatexPathPrefix = "none", latexOnly = false)

      val result = sb.toString()
      x.dataSet -> result
    }.toIndexedSeq.sortBy(_._1).foreach(x => mainSb.append(x._2).append("\n"))

//    Experiment.longExtractor(
//      experiments,
//      _.algorithmMetrics.wallClockPerStepPerWorker,
//      _.baseLineResults,
//      "graphalytics-overview-wallclock",
//      "Average WallClock"
//    )
//
//    Experiment.longExtractor(
//      experiments,
//      _.algorithmMetrics.activeVerticesPerStepPerWorker,
//      _.baseLineResults,
//      "graphalytics-overview-active-vertices",
//      "Average Active vertices"
//    )

//    Experiment.statisticsExtractor(
//      experiments,
//      _.algorithmMetrics.offHeapMemoryPerStepPerWorker,
//      _.baseLineResults,
//      "graphalytics-overview-offheap-memory",
//      "Average OffHeap-Memory"
//    )
//
//    Experiment.statisticsExtractor(
//      experiments,
//      _.algorithmMetrics.heapMemoryUsedPerStepPerWorker,
//      _.baseLineResults,
//      "graphalytics-overview-onheap-memory",
//      "Average OnHeap-Memory"
//    )
//
//    Experiment.statisticsExtractor(
//      experiments,
//      _.algorithmMetrics.averageLoadPerStepPerWorker,
//      _.baseLineResults,
//      "graphalytics-overview-cpu-load-memory",
//      "Average CPU Load"
//    )
  }

  groupedExperiments.foreach {
    case (dataSet, experiments) =>
      val mainSb = StringBuilder.newBuilder
      buildBaseCrampedPerAlgorithm(experiments, mainSb)
      dataSet -> mainSb.toString
  }
}
