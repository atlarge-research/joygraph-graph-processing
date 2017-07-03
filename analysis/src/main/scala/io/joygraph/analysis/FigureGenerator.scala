package io.joygraph.analysis

import io.joygraph.analysis.matplotlib.VariabilityBarPerStep

import scala.collection.parallel.ParIterable
import scala.reflect.io.{Directory, File}

object FigureGenerator extends App {
  val baseResultDirectory = "/home/sietse/Documents/experimental-results/elastic"
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
  val targetFigDir = s"/home/sietse/Documents/Thesis/LateX/$relativeFigPathDir"
  val elasticityResultsTexFile = File("/home/sietse/Documents/Thesis/LateX/elasticityresults.tex")
//  elasticityResultsTexFile.writeAll("") // empty file

  val results = ParseResultDirectories(resultsDirs)
  val groupedExperiments = results.experiments.groupBy(_.dataSet)
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
      sb.append(x.createVerticesPerStepDiagrams("activeverticesperstepperworker-%s-%s".format(x.algorithm,x.dataSet))).append("\n")
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

  groupedExperiments.map {
    case (dataSet, experiments) =>
      val mainSb = StringBuilder.newBuilder
//      mainSb.append("\\subsection{Active vertices per algorithm for %s}".format(dataSet)).append("\n")
//      buildAlgorithmStatistics(experiments, mainSb)
      buildPerExperimentActiveVerticesPerStepPerWorker(experiments, mainSb)
      mainSb.append("\\subsection")
      mainSb.append("\\subsubsection{Performance and elasticity metrics for %s}".format(dataSet)).append("\n")
//      buildPerformanceAndElasticityMetrics(experiments, mainSb)
      mainSb.append("\\newpage")
      mainSb.append("\\subsubsection{Supply-demand and variability plots for %s}".format(dataSet)).append("\n")
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
