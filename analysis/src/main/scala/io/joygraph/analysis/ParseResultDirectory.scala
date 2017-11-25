package io.joygraph.analysis

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.Properties

import io.joygraph.analysis.algorithm.AlgorithmMetric
import io.joygraph.analysis.autoscale.AutoscalerMetricCalculator
import io.joygraph.analysis.autoscale.metrics.{AccuracyMetric, InstabilityMetric, WrongProvisioningMetric}
import io.joygraph.analysis.performance.PerformanceMetric
import io.joygraph.core.actor.metrics.{SupplyDemandMetrics, WorkerOperation, WorkerState}

import scala.collection.immutable
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable.{ParIterable, ParMap, ParSeq}
import scala.io.Source
import scala.reflect.io.{Directory, File, Path}
import scala.util.{Failure, Success, Try}

trait BaseResultProperties {
  val dir : Directory
  val benchmarkId: String = dir.name
  val benchmarkLog: Path = dir / "benchmark-log.txt"
  val joygraphPropertiesPath: Path = dir / "config" / "joygraph.properties"
  val metricsDir: Path = dir / s"metrics_$benchmarkId"
  val metricsFile: Path = metricsDir / "metrics.bin"
  val nodeLogsDir: Directory = (dir / "joblog" / "yarnlog").toDirectory

  val graphPropertiesFile: Properties = {
    val p = new Properties()
    p.load(new FileInputStream((dir / "config" / "runs" / "all.properties").jfile))
    p
  }

  val joygraphPropertiesFile: Properties = {
    val p = new Properties()
    p.load(new FileInputStream(joygraphPropertiesPath.jfile))
    p
  }

  val algorithmName: String = getAlgorithmName
  val datasetName: String = getDatasetName
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
  val dirs: IndexedSeq[Directory] = dir.dirs.toIndexedSeq
  val files: IndexedSeq[File] = dir.files.toIndexedSeq
  val valid: Option[String] = Source.fromFile(benchmarkLog.jfile).getLines().find(_.contains("Validation successful"))
  if (valid.isEmpty) throw new IllegalArgumentException("Invalid Result")
  val metrics = Try {
    MetricsTransformer(metricsFile.jfile.getAbsolutePath)
  } match {
    case Failure(exception) =>
      throw new Exception(s"Could not find metrics file for $getAlgorithmName $getDatasetName ${metricsFile.jfile.getAbsolutePath}")
    case Success(value) =>
      value
  }
  val algorithmMetrics: AlgorithmMetric = AlgorithmMetric.calculate(metrics.policyMetricsReader, benchmarkId)
  val masterNodeStdout: Option[File] = nodeLogsDir.deepFiles.find(_.name == "appMaster.jar.stdout")
  val times: Iterator[Long] = Source.fromFile(benchmarkLog.jfile).getLines().flatMap(s =>
    if (s.startsWith("ProcessingTime")) {
      Some(s.split(":")(1).trim.toLong)
    } else if (s.endsWith(" ms.")) {
      Some(s.split("took ")(1).stripSuffix(" ms.").toLong)
    } else {
      None
    }
  )

  def initialNumberOfWorkers() : Int = {
    metrics.policyMetricsReader.workersForStep(0).size
  }

  val experimentDate: Long = experimentDateCalc()

  val (processingTime, makeSpan) = (times.next / 1000L, times.next / 1000L)
  lazy val machineTime: Long = machineTimeCalc() / 1000L

  val machineProcessingTime: Long = machineProcessingTimeCalc() / 1000L
  val machineElasticOverheadTime: Long = machineElasticOverheadCalc() / 1000L
  val superStepTimeSum: Long = superStepTimeSumCalc() / 1000L
  val elasticTime: Long = elasticTimeCalc() / 1000L

  lazy val performanceMetrics = PerformanceMetric(processingTime, makeSpan,
    elasticTime,
    machineTime,
    vertices / processingTime, edges / processingTime,
    machineElasticOverheadTime,
    superStepTimeSum)

  def experimentDateCalc() : Long = {
    val format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    val fileName = dirs.filter(_.name.startsWith("joygraph-report-")).iterator.next().name
    val dateRaw = fileName.substring("joygraph-report-".length)

    // transform to proper format
    val year = "20" + dateRaw.substring(0, 2)
    val month = dateRaw.substring(2, 4)
    val day = dateRaw.substring(4,6)
    val hour = dateRaw.substring(7, 9)
    val minutes = dateRaw.substring(9, 11)
    val seconds = dateRaw.substring(11, 13)
    format.parse(s"$month/$day/$year $hour:$minutes:$seconds").getTime
  }

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
    val warnSec = "[WARN] [SECURITY]"
    if (line.startsWith("[E")) {
      line.substring(9, 32)
    } else if (line.startsWith(warnSec)) {
      line.substring(warnSec.length + 1, warnSec.length + 24)
    } else {
      line.substring(8, 31)
    }
  }

  def averageTimesPerStepOf(workerOperation : WorkerOperation.Value) : Iterable[(Int, Double)] = {
    val reader = metrics.policyMetricsReader
    reader.rawStates(workerOperation).flatMap { workerStates =>
      if (workerStates.nonEmpty) {
        val workerProcessingTimes = workerStates.map {
          case WorkerState(_, start, Some(stop)) =>
            stop - start
        }
        Some(
          workerProcessingTimes.sum.toDouble / workerProcessingTimes.size
        )
      } else {
        None
      }
    }.zipWithIndex.map(_.swap)
  }

  def startStopTimesOf(workerOperation: WorkerOperation.Value) : Iterable[(Int, Option[(Long, Long)])] = {
    val reader = metrics.policyMetricsReader
    reader.rawStates(workerOperation).zipWithIndex.map{ case (workerStates, step) =>
      if (workerStates.nonEmpty) {
        step -> Some(workerStates.reduce[WorkerState] {
          case (WorkerState(_, start1, stop1), WorkerState(_, start2, stop2)) =>
            WorkerState(workerOperation, math.min(start1, start2), Some(math.max(stop1.get, stop2.get)))
        })
      } else {
        step -> None
      }
    }.map{
      case (step, Some(WorkerState(_, start, Some(stop)))) =>
        (step, Some(start, stop))
      case (step, None) => step -> None
    }
  }

  private[this] def sortedSupplyDemands() : ArrayBuffer[SupplyDemandMetrics] = {
    val reader = metrics.policyMetricsReader
    reader.supplyDemands.sortBy(_.timeMs)
  }

  def supplyTimeMs() : Iterable[(Long, Int)] = {
    val supplyDemands = sortedSupplyDemands()
    val supplyDemandPairs = supplyDemands.zip(supplyDemands.tail)
    val SupplyDemandMetrics(_, lastTime, lastSupply, lastDemand) = supplyDemands.last
    val supplies = supplyDemandPairs.flatMap {
      case (SupplyDemandMetrics(superStep, timeMs, supply, demand), SupplyDemandMetrics(superStep2, timeMs2, supply2, demand2)) =>
        Iterable(timeMs -> supply, timeMs2 -> supply)
    } ++ Iterable(lastTime -> lastSupply)

    supplies
  }

  def demandTimeMs() : Iterable[(Long, Int)] = {
    val supplyDemands = sortedSupplyDemands()
    val supplyDemandPairs = supplyDemands.zip(supplyDemands.tail)
    val SupplyDemandMetrics(_, lastTime, lastSupply, lastDemand) = supplyDemands.last
    val demands = supplyDemandPairs.flatMap {
      case (SupplyDemandMetrics(superStep, timeMs, supply, demand), SupplyDemandMetrics(superStep2, timeMs2, supply2, demand2)) =>
        if (superStep != superStep2) {
          Iterable(timeMs -> demand, timeMs2 -> demand)
        } else {
          Iterable(timeMs -> demand)
        }
    } ++ Iterable(lastTime -> lastDemand)

    demands
  }

  def elasticTimeCalc() : Long = {
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

    val elasticTimes = for (step <- 0 to reader.totalNumberOfSteps()) yield {
      val workers = reader.workersForStep(step)

      val longestWorkerTime = growShrinks.get(step) match {
        case Some(_) =>
          workers.flatMap { worker =>
            reader.timeOfOperation(step, worker, WorkerOperation.DISTRIBUTE_DATA)
          }.max
        case None =>
          0
      }
      longestWorkerTime
    }

    elasticTimes.sum
  }

  def machineElasticOverheadTimes(): IndexedSeq[Long] = {
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

    for (step <- 0 to reader.totalNumberOfSteps()) yield {
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
  }

  def machineElasticOverheadCalc() : Long = {
    val overheadTimes = machineElasticOverheadTimes()
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
  val policyClassName: String = findPolicyClassName()
  if (policyClassName == "NONE") throw new IllegalArgumentException
  val policyName: String = transformPolicyName(policyClassName)
  val maxWorkerCount: Int = findMaxWorkerCount()
  val accMetric: AccuracyMetric = AutoscalerMetricCalculator.getAccuracyMetric(metrics.policyMetricsReader, maxWorkerCount)
  val wrongProvisioningMetric: WrongProvisioningMetric = AutoscalerMetricCalculator.getWrongProvisioningMetric(metrics.policyMetricsReader)
  val instabilityMetric: InstabilityMetric = AutoscalerMetricCalculator.getInstabilityMetric(metrics.policyMetricsReader)

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

  lazy val experiments: immutable.Iterable[Experiment] = results.map{case ((datasetName, algorithmName), res) => Experiment(datasetName, algorithmName, res)}

}

case class ParseResultDirectories(resultsDirs : Iterable[String]) {
  val directories: Array[Directory] = resultsDirs.map(resultsDir => Directory(Path(resultsDir))).map(_.dirs).reduce(_ ++ _).toArray
  val results: ParMap[(String, String), ParSeq[ExperimentalResult]] = directories.toIndexedSeq.par.flatMap{
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
  val experiments: ParIterable[Experiment] = results.map{case ((datasetName, algorithmName), res) => Experiment(datasetName, algorithmName, res.toIndexedSeq)}
}
