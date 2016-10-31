package io.joygraph.analysis

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.Properties

import io.joygraph.analysis.autoscale.AutoscalerMetricCalculator
import io.joygraph.analysis.performance.PerformanceMetric
import io.joygraph.core.actor.metrics.WorkerOperation

import scala.collection.immutable.IndexedSeq
import scala.io.Source
import scala.reflect.io.{Directory, Path}
import scala.util.{Failure, Success, Try}

trait BaseResultProperties {
  val dir : Directory
  val benchmarkId = dir.name
  val benchmarkLog = dir / "benchmark-log.txt"
  val joygraphPropertiesPath = dir / "config" / "joygraph.properties"
  val metricsDir = dir / s"metrics_$benchmarkId"
  val metricsFile = metricsDir / "metrics.bin"
  val nodeLogsDir = (dir / "joblog" / "yarnlog").toDirectory

  val graphPropertiesFile = {
    val p = new Properties()
    p.load(new FileInputStream((dir / "config" / "runs" / "all.properties").jfile))
    p
  }

  val joygraphPropertiesFile = {
    val p = new Properties()
    p.load(new FileInputStream(joygraphPropertiesPath.jfile))
    p
  }

  val algorithmName = getAlgorithmName
  val datasetName = getDatasetName
  val (vertices, edges) = getVerticesEdges

  def getDatasetName : String = {
    graphPropertiesFile.getProperty("benchmark.run.graphs")
  }

  def getAlgorithmName : String = {
    graphPropertiesFile.getProperty("benchmark.run.algorithms")
  }

  def getVerticesEdges : (Long, Long) = {
    val propertiesFile = (dir / "config" / "graphs" / (getDatasetName + ".properties")).jfile
    val properties = new Properties()
    properties.load(new FileInputStream(propertiesFile))
    val edges = properties.getProperty(s"graph.$datasetName.meta.edges").toLong
    val vertices = properties.getProperty(s"graph.$datasetName.meta.vertices").toLong
    if (properties.getProperty(s"graph.$datasetName.directed").toBoolean) {
      (vertices, edges)
    } else {
      (vertices, edges * 2)
    }
  }
}

trait GeneralResultProperties extends BaseResultProperties {
  val dirs = dir.dirs.toIndexedSeq
  val files = dir.files.toIndexedSeq
  val valid = Source.fromFile(benchmarkLog.jfile).getLines().find(_.contains("Validation successful"))
  if (valid.isEmpty) throw new IllegalArgumentException("Invalid Result")
  val metrics = MetricsTransformer(metricsFile.jfile.getAbsolutePath)
  val masterNodeStdout = nodeLogsDir.deepFiles.find(_.name == "appMaster.jar.stdout")
  val times = Source.fromFile(benchmarkLog.jfile).getLines().flatMap(s =>
    if (s.startsWith("ProcessingTime")) {
      Some(s.split(":")(1).trim.toLong)
    } else if (s.endsWith(" ms.")) {
      Some(s.split("took ")(1).stripSuffix(" ms.").toLong)
    } else {
      None
    }
  )

  val (processingTime, makeSpan) = (times.next / 1000L, times.next / 1000L)
  val machineTime = machineTimeCalc() / 1000L

  val machineProcessingTime = machineProcessingTimeCalc() / 1000L
  val machineElasticOverheadTime = machineElasticOverheadCalc() / 1000L
  val superStepTimeSum = superStepTimeSumCalc() / 1000L

  val performanceMetrics = PerformanceMetric(processingTime, makeSpan, machineTime,
    vertices / processingTime, edges / processingTime,
    machineElasticOverheadTime,
    superStepTimeSum)

  //  @deprecated
  //  def verticesEdges : (Long, Long) = {
  //    Source.fromFile(masterNodeStdout.get.jfile).getLines().find(x => x.contains("total : ")) match {
  //      case Some(x) =>
  //        val splits = x.split(" ")
  //        val vertices = splits(splits.length - 2)
  //        val edges = splits(splits.length - 1)
  //        (vertices.toLong, edges.toLong)
  //      case None =>
  //        (0,0)
  //    }
  //  }

  def superStepTimeSumCalc() : Long = {
    val reader = metrics.policyMetricsReader
    val superStepTimes = for (step <- 0 to reader.totalNumberOfSteps()) yield {
      reader.timeOfAllWorkersOfAction(step, WorkerOperation.RUN_SUPERSTEP)
    }
    superStepTimes.sum
  }

  def machineTimeCalc() : Long = {
    val format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS")
    // the machine time is the container time
    // we check the logs of each container
    val firstAndLastLines = nodeLogsDir.deepFiles.filter(file => file.name.startsWith("app") && file.name.endsWith("stdout")).map{
      file =>
        val lines = Source.fromFile(file.jfile).getLines()
        var firstLine = ""
        var lastLine = ""
        lines.foreach{ line =>
          if (line.startsWith("[")) {
            if (firstLine.isEmpty) {
              firstLine = line
            }
            lastLine = line
          }
        }
        (firstLine, lastLine)
    }.toIndexedSeq
//      [INFO] [09/27/2016 15:28:34.700]
    firstAndLastLines.map{
      case (firstLine, lastLine) =>
        val dateOne = timeFromString(firstLine)
        val dateTwo = timeFromString(lastLine)
        format.parse(dateTwo).getTime - format.parse(dateOne).getTime
    }.sum
  }

  private[this] def timeFromString(line : String) : String = {
    if (line.startsWith("[E")) {
      line.substring(9, 32)
    } else {
      line.substring(8, 31)
    }
  }

  def machineElasticOverheadCalc() : Long = {
    val reader = metrics.policyMetricsReader
    // get grow or shrink
    val growShrinks = reader.supplyDemands.flatMap{ x =>
      val sign = math.signum(x.demand - x.supply)
      if (sign == 0) {
        None
      } else {
        Some(x.superStep -> (x.supply, x.demand))
      }
    }.toMap

    val overheadTimes = for (step <- 0 to reader.totalNumberOfSteps()) yield {
      growShrinks.get(step) match {
        case Some((supply, demand)) =>
          val time = reader.timeOfAllWorkersOfAction(step, WorkerOperation.DISTRIBUTE_DATA)
          if (demand - supply > 0) {
            time / supply * demand
          } else {
            time
          }
        case None =>
          0
      }
    }
    overheadTimes.sum
  }

  def machineProcessingTimeCalc(): Long = {
    val reader = metrics.policyMetricsReader
    val processingTimes = for (step <- 0 to reader.totalNumberOfSteps()) yield {
      reader.timeOfAllWorkersOfAction(step, WorkerOperation.RUN_SUPERSTEP)
    }

    processingTimes.sum
  }
}

trait PolicyResultProperties extends ExperimentalResult with GeneralResultProperties {
  val policyClassName = findPolicyClassName()
  if (policyClassName == "NONE") throw new IllegalArgumentException
  val policyName = transformPolicyName(policyClassName)
  val maxWorkerCount = findMaxWorkerCount()
  val accMetric = AutoscalerMetricCalculator.getAccuracyMetric(metrics.policyMetricsReader, maxWorkerCount)
  val wrongProvisioningMetric = AutoscalerMetricCalculator.getWrongProvisioningMetric(metrics.policyMetricsReader)
  val instabilityMetric = AutoscalerMetricCalculator.getInstabilityMetric(metrics.policyMetricsReader)

  def transformPolicyName(policyClassName : String) : String = {
    policyClassName.substring(policyClassName.lastIndexOf(".") + 1) match {
      case name @ ("AKTE" | "ConPaaS" | "Reg" | "React" | "Hist") =>
        name
      case "WallClockPolicy" =>
        "WCP"
      case "CPUPolicy" =>
        "CPU"
      case "CPUPolicyV2" =>
        "CPUv2"
      case "NetworkPolicy" =>
        "NP"
      case _ =>
        "UNKNOWN"
    }
  }

  def findMaxWorkerCount(): Int = {
    Option(joygraphPropertiesFile.getProperty("joygraph.job.max-worker-count")) match {
      case Some(x) =>
        x.toInt
      case None =>
        -1
    }
  }

  def findPolicyClassName(): String = {
    Option(joygraphPropertiesFile.getProperty("joygraph.job.policy.name")) match {
      case Some(x) =>
        x
      case None =>
        "NONE"
    }
  }
}

class ExperimentalResult(val dir : Directory) extends BaseResultProperties {

}

case class ParseResultDirectory(resultsDir : String) {
  val directory = Directory(Path(resultsDir))

  val results: Map[(String, String), IndexedSeq[ExperimentalResult]] = directory.dirs.toIndexedSeq.flatMap{
    dir => Try[ExperimentalResult] {
      new ExperimentalResult(dir)
    } match {
      case Failure(exception) =>
        println(exception)
        None
      case Success(value) =>
        Some(value)
    }
  }.groupBy(x => (x.datasetName, x.algorithmName))

  lazy val experiments = results.map{case ((datasetName, algorithmName), res) => Experiment(datasetName, algorithmName, res)}

}

case class ParseResultDirectories(resultsDirs : Iterable[String]) {
  val directories: Iterator[Directory] = resultsDirs.map(resultsDir => Directory(Path(resultsDir))).map(_.dirs).reduce(_ ++ _)
  val results = directories.toIndexedSeq.par.flatMap{
    dir => Try[ExperimentalResult] {
      new ExperimentalResult(dir)
    } match {
      case Failure(exception) =>
        println(exception)
        None
      case Success(value) =>
        Some(value)
    }
  }.groupBy(x => (x.datasetName, x.algorithmName))
  val experiments = results.map{case ((datasetName, algorithmName), res) => Experiment(datasetName, algorithmName, res.toIndexedSeq)}
}