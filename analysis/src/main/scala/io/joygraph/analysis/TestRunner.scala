package io.joygraph.analysis

object TestRunner {
  def main(args : Array[String]): Unit = {
    //    val resultsDir = "/home/sietse/Documents/results-PYAUTOSCALE-2016-09-20-success"
    //    "result-test-own-policies-2016-09-27"
    //    //    val resultsDir = args(0)
    //    val results = ParseResultDirectory(resultsDir)
    //    results.experiments.foreach { x =>
    //      println(x.createPerformanceTableWithAverages())
    //      println(x.createElasticTableWithAverages())
    //    }

    val resultsDirs = Iterable(
//      "/home/sietse/Documents/results-PYAUTOSCALE-2016-09-20-success",
//      "/home/sietse/Documents/result-test-own-policies-2016-09-27",
      "/home/sietse/Documents/results-pr-gr25-own-autoscale-29-09-2016")

    //    val resultsDir = args(0)
    val results = ParseResultDirectories(resultsDirs)
    results.experiments.foreach { x =>
//      println(x.createPerformanceTableWithAverages())
//      println(x.createElasticTableWithAverages())
        println(x.createFigureVerticesPerSecondFigure(_.verticesPerSecond, "Policy", "Vertices/s", s"${x.algorithm} on ${x.dataSet}", "ass"))
      //      x.policyResults.foreach{
//        result =>
//          println(s"${result.benchmarkId}: ${result.metricsFile}")
//          Try[Unit] {
//            Option(result.joygraphPropertiesFile.getProperty("joygraph.job.policy.name")) match {
//              case Some(x) =>
//                println(x)
//              case None =>
//                "NONE"
//            }
//            val reader = ElasticPolicyReader(result.metricsFile.jfile.toString)
//            reader.supplyDemands.foreach(println)
//          } match {
//            case Failure(exception) =>
//              println(exception)
//            case Success(value) =>
//          }
//
//
//          println()
//      }
    }

//    val reader = ElasticPolicyReader("/home/sietse/Documents/result-test-own-policies-2016-09-27/b6722341281/metrics_b6722341281/metrics.bin")
//    (0 until reader.totalNumberOfSteps()).foreach{ step =>
//      reader.superStepMetricsAllWorkers(step).foreach{
//        case (workerId, metrics) =>
//          metrics.foreach{
//            case ass @ Cpu(_, time, load, combined, stolen, processors) =>
//              println(combined)
//              println(s"$workerId: $time $load $stolen $processors")
//          }
//      }
//    }

  }
}
