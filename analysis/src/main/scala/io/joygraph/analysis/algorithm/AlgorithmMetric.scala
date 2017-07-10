package io.joygraph.analysis.algorithm
import akka.cluster.metrics.Metric
import akka.cluster.metrics.StandardMetrics.{HeapMemoryUsed, SystemLoadAverage}
import io.joygraph.analysis.ElasticPolicyReader
import io.joygraph.core.actor.metrics.{NetworkMetrics, OffHeapMemoryMetrics}
import org.apache.commons.math3.stat.descriptive.moment.{Mean, StandardDeviation}

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.util.{Failure, Success, Try}

case class AlgorithmMetric
(activeVerticesPerStep : immutable.Seq[Long],
 wallClockPerStepPerWorker : immutable.Seq[Iterable[(Int, Long)]],
 activeVerticesPerStepPerWorker : immutable.Seq[Iterable[(Int, Long)]],
 heapMemoryUsedPerStepPerWorker : immutable.IndexedSeq[Iterable[(Int, Statistics)]],
 offHeapMemoryPerStepPerWorker : immutable.IndexedSeq[Iterable[(Int, Statistics)]],
 bytesReceivedPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, Statistics)]],
 bytesSentPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, Statistics)]],
 averageLoadPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, Statistics)]]
) {

}

case class Statistics(std : Double, average : Double, n : Long) {}

object AlgorithmMetric {

  private def extractGeneralMetrics[T <: Number]
  (name : String, transformer : Metric => T, metric : Metric) : Option[T] = {
    metric.name match {
      case `name` =>
        Some(transformer(metric))
      case _ =>
        None
    }
  }

  private def extractGeneralMetricsPerStepPerWorker[T, A]
  (elasticPolicyReader: ElasticPolicyReader,
   transformer : Metric => Option[T],
   aggregator : Iterable[T] => A
  ): immutable.IndexedSeq[Iterable[(Int, A)]] = {
    for (i <- 0 until elasticPolicyReader.totalNumberOfSteps())
      yield {
        for (workerId <- elasticPolicyReader.workersForStep(i))
          yield {
            workerId -> {
              aggregator(elasticPolicyReader.superStepWorkerMetrics(i, workerId).map { nodeMetrics =>
                nodeMetrics.getMetrics.flatMap { transformer(_) }
              }.foldLeft(Iterable.empty[T])(_ ++ _))
            }
          }
      }
  }

  private def extractGeneralMetricsPerStepPerWorkerAggregateLong
  (elasticPolicyReader: ElasticPolicyReader,
   transformer : Metric => Option[Long]
  ): immutable.IndexedSeq[Iterable[(Int, Statistics)]] = {
    extractGeneralMetricsPerStepPerWorker[Long, Statistics](elasticPolicyReader,
      transformer,
      collection => {
        val rawArray = collection.map(_.toDouble).toArray
        val std = new StandardDeviation(true)
        val mean = new Mean()
        Try[Statistics] {
          Statistics(
            std.evaluate(rawArray),
            mean.evaluate(rawArray),
            rawArray.length
          )
        } match {
          case Failure(exception) =>
            throw exception;
          case Success(value) =>
            value
        }

      }
    )
  }


  private def aggregated[T]
  (
    rawPerWorker: Iterable[(Int, Iterable[T])],
    transformer : Iterable[T] => Statistics
  ) : Iterable[(Int, Statistics)]= {
    rawPerWorker.map{
      case (workerId, collection) =>
        workerId -> transformer(collection)
    }
  }

  def calculate(policyMetricsReader : ElasticPolicyReader, benchmarkId : String): AlgorithmMetric = {
    val activeVerticesPerStep : immutable.Seq[Long] = {
      for (i <- 0 until policyMetricsReader.totalNumberOfSteps())
        yield policyMetricsReader.activeVerticesSumOf(i)
    }

    val activeVerticesPerStepPerWorker: immutable.Seq[Iterable[(Int, Long)]] = {
      for (i <- 0 until policyMetricsReader.totalNumberOfSteps())
        yield {
          for (workerId <- policyMetricsReader.workersForStep(i))
            yield {
              workerId -> {
                policyMetricsReader.activeVerticesOf(i, workerId) match {
                  case Some(x) =>
                    x
                  case None =>
                    0
                }
              }
            }
        }
    }

    val wallClockPerStepPerWorker : immutable.Seq[Iterable[(Int, Long)]] = {
      for (i <- 0 until policyMetricsReader.totalNumberOfSteps())
        yield {
          for (workerId <- policyMetricsReader.workersForStep(i))
            yield {
              workerId -> policyMetricsReader.stepTime(i, workerId)
            }
        }
    }

    val averageOffHeapMemoryPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, Statistics)]] = {
      extractGeneralMetricsPerStepPerWorkerAggregateLong(policyMetricsReader,
          metric =>
            metric.name match {
            case OffHeapMemoryMetrics.OffHeapMemoryUsed =>
              Some[Long](metric.value.longValue())
            case _ =>
              None
          }
        )
    }

    val averageHeapMemoryPerStepPerWorker : immutable.IndexedSeq[Iterable[(Int, Statistics)]] = {
      extractGeneralMetricsPerStepPerWorkerAggregateLong(policyMetricsReader,
        metric =>
          metric.name match {
            case HeapMemoryUsed =>
              Some[Long](metric.value.longValue())
            case _ =>
              None
          }
      )
    }

    val bytesSentPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, Statistics)]] = {
      extractGeneralMetricsPerStepPerWorkerAggregateLong(policyMetricsReader,
        metric =>
          metric.name match {
            case NetworkMetrics.BytesSent =>
              Some[Long](metric.value.longValue())
            case _ =>
              None
          }
      )
    }

    val bytesReceivedPerStepPerWorker : immutable.IndexedSeq[Iterable[(Int, Statistics)]] = {
      extractGeneralMetricsPerStepPerWorkerAggregateLong(policyMetricsReader,
        metric =>
          metric.name match {
            case NetworkMetrics.BytesReceived =>
              Some[Long](metric.value.longValue())
            case _ =>
              None
          }
      )
    }

    val averageLoadPerStepPerWorker : immutable.IndexedSeq[Iterable[(Int, Statistics)]] = {
      extractGeneralMetricsPerStepPerWorkerAggregateLong(policyMetricsReader,
        metric =>
          metric.name match {
            case SystemLoadAverage =>
              Some[Long](metric.value.longValue())
            case _ =>
              None
          }
      )
    }

    AlgorithmMetric(
      activeVerticesPerStep,
      wallClockPerStepPerWorker,
      activeVerticesPerStepPerWorker,
      averageHeapMemoryPerStepPerWorker,
      averageOffHeapMemoryPerStepPerWorker,
      bytesReceivedPerStepPerWorker,
      bytesSentPerStepPerWorker,
      averageLoadPerStepPerWorker
    )
  }
}
