package io.joygraph.analysis

import java.util.Properties

import scala.collection.parallel.immutable.ParMap
import scala.collection.parallel.{ParIterable, immutable}
import scala.reflect.io.{Directory, File}

object FigureGenerator extends App {
  val propertiesConfig = {
    val prop = new Properties()
    prop.load(FigureGenerator.getClass.getResourceAsStream("/fig-generator.properties"))
    prop
  }

  val baseResultDirectory = propertiesConfig.getProperty("basedir")
  val bfsResultDirs = baseResultDirectory + "/BFS"
  val prResultDirs = baseResultDirectory + "/PR"
  val wccResultDirs = baseResultDirectory + "/WCC"
  val gr26ResultDirs = baseResultDirectory + "/GR26"

  val resultsDirs = Iterable(
    bfsResultDirs,
    prResultDirs,
    wccResultDirs,
    gr26ResultDirs
  ).map(Directory(_)).flatMap(_.dirs.map(_.toFile.toString()))

  val relativeFigPathDir = "elastic-figs"
  val targetFigDir = s"${propertiesConfig.getProperty("targetfigdir")}/$relativeFigPathDir"
  val elasticityResultsTexFile = File(propertiesConfig.getProperty("elasticityresultstex"))
//  elasticityResultsTexFile.writeAll("") // empty file

  val results = ParseResultDirectories(resultsDirs)
  val groupedExperiments: ParMap[String, immutable.ParIterable[Experiment]] = results.experiments.groupBy(_.dataSet)
  val LATEXONLY = true

  def buildPerformanceAndElasticityMetrics(experiments : ParIterable[Experiment], mainSb : StringBuilder): Unit = {
    experiments.map { x =>
      val sb = StringBuilder.newBuilder
      sb.append(x.createPerformanceTableWithAverages()).append("\n")
      sb.append(x.createElasticTableWithAverages()).append("\n")
      sb.append(x.createTournamentScoreTableMerged()).append("\n")
      val result = sb.toString
      sb.clear()
      x.algorithm -> result
    }.toIndexedSeq.sortBy(_._1).foreach(x => mainSb.append(x._2).append("\n"))
  }

  def buildPerExperimentActiveVerticesPerStepPerWorker(experiments : ParIterable[Experiment], mainSb : StringBuilder) : Unit = {
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
      val result = sb.toString()
      x.dataSet -> result
    }.toIndexedSeq.sortBy(_._1).foreach(x => mainSb.append(x._2).append("\n"))
  }

  def buildAlgorithmStatistics(experiments : ParIterable[Experiment], mainSb : StringBuilder): Unit = {
    // get all the algorithms, the active vertices are invariant per step
    experiments.map { x =>
      val sb = StringBuilder.newBuilder
      sb.append(x.createPerStepDiagrams("activeverticesperstep-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
      val result = sb.toString()
      x.dataSet -> result
    }.toIndexedSeq.sortBy(_._1).foreach(x => mainSb.append(x._2).append("\n"))
  }

  def buildDiagrams(experiments : ParIterable[Experiment], mainSb : StringBuilder): Unit = {
    experiments.map { x =>
      val sb = StringBuilder.newBuilder
      val supplyDemandPlotFilePathPrefix = s"$targetFigDir"
      val latexFigs = x.createSupplyDemandPlot(supplyDemandPlotFilePathPrefix, relativeFigPathDir, LATEXONLY)
      latexFigs.foreach{ fig =>
        sb.append(fig).append("\n")
      }
      val result = sb.toString
      sb.clear()
      x.algorithm -> result
    }.toIndexedSeq.sortBy(_._1).foreach(x => mainSb.append(x._2).append("\n"))
  }

  def buildBaseCrampedPerAlgorithm(experiments : ParIterable[Experiment], mainSb : StringBuilder) : Unit = {
    Experiment.longExtractor(
      experiments,
      _.algorithmMetrics.wallClockPerStepPerWorker,
      _.baseLineResults,
      "base-overview-wallclock",
      "Average WallClock"
    )

    Experiment.longExtractor(
      experiments,
      _.algorithmMetrics.activeVerticesPerStepPerWorker,
      _.baseLineResults,
      "base-overview-active-vertices",
      "Average Active vertices"
    )

    Experiment.statisticsExtractor(
      experiments,
      _.algorithmMetrics.offHeapMemoryPerStepPerWorker,
      _.baseLineResults,
      "base-overview-offheap-memory",
      "Average OffHeap-Memory"
    )

    Experiment.statisticsExtractor(
      experiments,
      _.algorithmMetrics.heapMemoryUsedPerStepPerWorker,
      _.baseLineResults,
      "base-overview-onheap-memory",
      "Average OnHeap-Memory"
    )

    Experiment.statisticsExtractor(
      experiments,
      _.algorithmMetrics.averageLoadPerStepPerWorker,
      _.baseLineResults,
      "base-overview-cpu-load-memory",
      "Average CPU Load"
    )
  }

  groupedExperiments.map {
    case (dataSet, experiments) =>
      val mainSb = StringBuilder.newBuilder
//      mainSb.append("\\subsection{Active vertices per algorithm for %s}".format(dataSet)).append("\n")
//      buildAlgorithmStatistics(experiments, mainSb)
//      buildPerExperimentActiveVerticesPerStepPerWorker(experiments, mainSb)
      buildBaseCrampedPerAlgorithm(experiments, mainSb)
//      mainSb.append("\\subsection")
//      mainSb.append("\\subsubsection{Performance and elasticity metrics for %s}".format(dataSet)).append("\n")
//      buildPerformanceAndElasticityMetrics(experiments, mainSb)
//      mainSb.append("\\newpage")
//      mainSb.append("\\subsubsection{Supply-demand and variability plots for %s}".format(dataSet)).append("\n")
//      buildDiagrams(experiments, mainSb)

//        sb.append(x.createTournamentScoreTableElastic()).append("\n")
//        sb.append(x.createTournamentScoreTablePerformance()).append("\n")
//        sb.append(x.createTournamentScoreTableCombined()).append("\n")

//        val namingTemplate = (element : String) => s"${x.dataSet}-${x.algorithm}-$element"
//        println(x.createFigureVerticesPerSecondFigure(z => z.verticesPerSecond + z.edgesPerSecond, "Policy", "Edges+Vertices/s", s"${x.algorithm} on ${x.dataSet}", namingTemplate("evps")))
//        println(x.createFigureVerticesPerSecondFigure(_.edgesPerSecond, "Policy", "Edges/s", s"${x.algorithm} on ${x.dataSet}", namingTemplate("eps")))
//        println(x.createFigureVerticesPerSecondFigure(_.processingTime, "Policy", "Processing time (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("proc")))
//        println(x.createFigureVerticesPerSecondFigure(_.machineTime, "Policy", "Machine time (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("machine")))
//        println(x.createFigureVerticesPerSecondFigure(_.elasticityOverhead, "Policy", "Elasticity overhead (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("elasticity-overhead")))
//        println(x.createFigureVerticesPerSecondFigure(_.makeSpan, "Policy", "Makespan(s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("mk")))

      dataSet -> mainSb.toString
  }.toIndexedSeq.sortBy(_._1).foreach(x => elasticityResultsTexFile.appendAll(x._2))
}
