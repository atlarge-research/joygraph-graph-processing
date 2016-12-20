package io.joygraph.analysis

import scala.reflect.io.{Directory, File}

object FigureGenerator extends App {
  val baseResultDirectory = "/home/sietse/thesis/experimental-results/elastic"
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
  val targetFigDir = s"/home/sietse/thesis/Thesis/LateX/${relativeFigPathDir}"
  val elasticityResultsTexFile = File("/home/sietse/thesis/Thesis/LateX/elasticityresults.tex")
  elasticityResultsTexFile.writeAll("") // empty file

  val results = ParseResultDirectories(resultsDirs)
  val groupedExperiments = results.experiments.groupBy(_.dataSet)
  groupedExperiments.map {
    case (dataSet, experiments) =>
      val mainSb = StringBuilder.newBuilder
      mainSb.append("\\subsubsection{%s}".format(dataSet)).append("\n")

      experiments.map { x =>
        val sb = StringBuilder.newBuilder
        val supplyDemandPlotFilePathPrefix = s"$targetFigDir"
        val latexFigs = x.createSupplyDemandPlot(supplyDemandPlotFilePathPrefix, relativeFigPathDir)
        sb.append(x.createPerformanceTableWithAverages()).append("\n")
        sb.append(x.createElasticTableWithAverages()).append("\n")
        sb.append(x.createTournamentScoreTableMerged()).append("\n")
//        sb.append(x.createTournamentScoreTableElastic()).append("\n")
//        sb.append(x.createTournamentScoreTablePerformance()).append("\n")
//        sb.append(x.createTournamentScoreTableCombined()).append("\n")
        latexFigs.foreach{ fig =>
          sb.append(fig).append("\n")
        }

        val namingTemplate = (element : String) => s"${x.dataSet}-${x.algorithm}-$element"
//        println(x.createFigureVerticesPerSecondFigure(z => z.verticesPerSecond + z.edgesPerSecond, "Policy", "Edges+Vertices/s", s"${x.algorithm} on ${x.dataSet}", namingTemplate("evps")))
//        println(x.createFigureVerticesPerSecondFigure(_.edgesPerSecond, "Policy", "Edges/s", s"${x.algorithm} on ${x.dataSet}", namingTemplate("eps")))
//        println(x.createFigureVerticesPerSecondFigure(_.processingTime, "Policy", "Processing time (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("proc")))
//        println(x.createFigureVerticesPerSecondFigure(_.machineTime, "Policy", "Machine time (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("machine")))
//        println(x.createFigureVerticesPerSecondFigure(_.elasticityOverhead, "Policy", "Elasticity overhead (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("elasticity-overhead")))
//        println(x.createFigureVerticesPerSecondFigure(_.makeSpan, "Policy", "Makespan(s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("mk")))
        x.algorithm -> sb.toString
      }.toIndexedSeq.sortBy(_._1).foreach(x => mainSb.append(x._2).append("\n"))
      dataSet -> mainSb.toString
  }.toIndexedSeq.sortBy(_._1).foreach(x => elasticityResultsTexFile.appendAll(x._2))
}
