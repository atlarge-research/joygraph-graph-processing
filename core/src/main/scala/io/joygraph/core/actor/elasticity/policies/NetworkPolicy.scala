package io.joygraph.core.actor.elasticity.policies
import io.joygraph.core.actor.metrics.{Network, WorkerOperation}
import io.joygraph.core.message.AddressPair

import scala.collection.mutable

class NetworkPolicy extends DefaultAveragingPolicy[Double] {

  println("NetworkPolicy")

  val bytesSentStepToWorker = mutable.Map.empty[Int, mutable.Map[Int, Long]]

  override protected[this] def calculateAverage(step: Int, currentWorkers: Iterable[Int]): Option[Double] = {
    Some(ElasticPolicy.average(individualWorkerValues(step, currentWorkers).map(_._2)))
  }

  override protected[this] def individualWorkerValues(step: Int, currentWorkers: Iterable[Int]): Iterable[(Int, Double)] = {
    // we want to check the average network load going down, so the average network load
    // we must normalize against the relative load,
    // so the relative load would be the load of a worker against the maximum load of a worker

    val workerBytesSent = currentWorkers.flatMap { workerId =>
      // get processing time
      val metrics = metricsOf(step, workerId, WorkerOperation.RUN_SUPERSTEP).get
      val bytesSentMetrics = metrics.flatMap {
        case Network(_,_,_, bytesSent) =>
          Some(bytesSent)
        case _ =>
          None
      }

      if (bytesSentMetrics.nonEmpty) {
        val max = bytesSentMetrics.max
        val prevValue = bytesSentStepToWorker
          .getOrElseUpdate(step - 1, mutable.Map.empty[Int, Long])
          .getOrElse(workerId, 0L)
        if (max < prevValue) {
          bytesSentStepToWorker
            .getOrElseUpdate(step, mutable.Map.empty[Int, Long])
            .put(workerId, prevValue)
        } else {
          bytesSentStepToWorker
            .getOrElseUpdate(step, mutable.Map.empty[Int, Long])
            .put(workerId, max)
        }

      }

      val prevValue: Long = bytesSentStepToWorker
        .getOrElseUpdate(step - 1, mutable.Map.empty[Int, Long])
        .getOrElse(workerId, 0L)

      val currentValue: Long = bytesSentStepToWorker
        .getOrElseUpdate(step, mutable.Map.empty[Int, Long])
        .getOrElse(workerId, 0L)

      val diff = currentValue - prevValue

      Some(workerId -> diff)
    }

    if (workerBytesSent.isEmpty) {
      Iterable.empty
    } else {
      val maxBytesSentByAWorker = workerBytesSent.maxBy(_._2)._2
      workerBytesSent.map{
        case (workerId, bytesSent) =>
          val ratio = if (maxBytesSentByAWorker > 0L) {
            bytesSent.toDouble / maxBytesSentByAWorker.toDouble
          } else {
            0.0
          }
          workerId -> ratio
      }
    }
  }

  /**
    * Returns 1, if it has a positive impact given the evaluation function, negative impact -1 and there was no decision 0
    */
  override protected[this] def evaluatePreviousDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Int = {
    // we try to minimize the difference between the highest and the lowest load
    // therefore we want the average ratio to be as high as possible
    decision(previousStep(currentStep)) match {
      case Some(x) =>
        // it seems that nothing changed or it got worse.
        if (stepAverage(currentStep).get <= stepAverage(previousStep(currentStep)).get) {
          -1
        } else {
          1
        }
      case None =>
        0
    }
  }

  override def enoughInformationToMakeDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Boolean = {
    individualWorkerValues(currentStep, currentWorkers.keys).size == currentWorkers.size
  }
}
