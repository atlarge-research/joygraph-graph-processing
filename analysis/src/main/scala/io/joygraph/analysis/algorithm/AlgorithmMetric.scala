package io.joygraph.analysis.algorithm
import akka.cluster.metrics.Metric
import io.joygraph.analysis.ElasticPolicyReader
import io.joygraph.core.actor.metrics.{NetworkMetrics, OffHeapMemoryMetrics}

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

case class AlgorithmMetric
(activeVerticesPerStep : immutable.Seq[Long],
 activeVerticesPerStepPerWorker : immutable.Seq[Iterable[(Int, Option[Long])]],
 offHeapMemoryPerStepPerWorker : immutable.IndexedSeq[Iterable[(Int, ArrayBuffer[Iterable[Long]])]],
 bytesSentPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, ArrayBuffer[Iterable[Long]])]],
 bytesReceivedPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, ArrayBuffer[Iterable[Long]])]],
 wallClockPerStepPerWorker : immutable.Seq[Iterable[(Int, Long)]]
) {

}

case class Statistics(std : Double, average : Double, n : Long) {}

object AlgorithmMetric {

  private def extractGeneralMetrics[T : Number]
  (name : String, transformer : Metric => T, metric : Metric) : Option[T] = {
    metric.name match {
      case `name` =>
        Some(transformer(metric))
      case _ =>
        None
    }
  }

  private def extractGeneralMetricsPerStepPerWorker[T]
  (elasticPolicyReader: ElasticPolicyReader, transformer : Metric => Option[T]): immutable.IndexedSeq[Iterable[(Int, ArrayBuffer[Iterable[T]])]] = {
    for (i <- 0 until elasticPolicyReader.totalNumberOfSteps())
      yield {
        for (workerId <- elasticPolicyReader.workersForStep(i))
          yield {
            workerId -> {
              elasticPolicyReader.superStepWorkerMetrics(i, workerId).map { nodeMetrics =>
                nodeMetrics.getMetrics.flatMap { transformer(_) }
              }
            }
          }
      }
  }

  def calculate(policyMetricsReader : ElasticPolicyReader): AlgorithmMetric = {
    val activeVerticesPerStep : immutable.Seq[Long] = {
      for (i <- 0 until policyMetricsReader.totalNumberOfSteps())
        yield policyMetricsReader.activeVerticesSumOf(i)
    }

    val activeVerticesPerStepPerWorker: immutable.Seq[Iterable[(Int, Option[Long])]] = {
      for (i <- 0 until policyMetricsReader.totalNumberOfSteps())
        yield {
          for (workerId <- policyMetricsReader.workersForStep(i))
            yield {
              workerId -> policyMetricsReader.activeVerticesOf(i, workerId)
            }
        }
    }
    val averageOffHeapMemoryPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, ArrayBuffer[Iterable[Long]])]] = {
        extractGeneralMetricsPerStepPerWorker(policyMetricsReader,
          metric =>
            metric.name match {
            case OffHeapMemoryMetrics.OffHeapMemoryUsed =>
              Some[Long](metric.value.longValue())
            case _ =>
              None
          }
        )
    }

    val bytesSentPerStepPerWorker: immutable.IndexedSeq[Iterable[(Int, ArrayBuffer[Iterable[Long]])]] = {
      extractGeneralMetricsPerStepPerWorker(policyMetricsReader,
        metric =>
          metric.name match {
            case NetworkMetrics.BytesSent =>
              Some[Long](metric.value.longValue())
            case _ =>
              None
          }
      )
    }

    val bytesReceivedPerStepPerWorker : immutable.IndexedSeq[Iterable[(Int, ArrayBuffer[Iterable[Long]])]] = {
      extractGeneralMetricsPerStepPerWorker(policyMetricsReader,
        metric =>
          metric.name match {
            case NetworkMetrics.BytesReceived =>
              Some[Long](metric.value.longValue())
            case _ =>
              None
          }
      )
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

    AlgorithmMetric(
      activeVerticesPerStep,
      activeVerticesPerStepPerWorker,
      averageOffHeapMemoryPerStepPerWorker,
      bytesSentPerStepPerWorker,
      bytesReceivedPerStepPerWorker,
      wallClockPerStepPerWorker
    )
  }
}
