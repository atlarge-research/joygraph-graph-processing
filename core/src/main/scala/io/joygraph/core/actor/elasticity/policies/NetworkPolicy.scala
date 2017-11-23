package io.joygraph.core.actor.elasticity.policies
import io.joygraph.core.actor.metrics._
import io.joygraph.core.message.AddressPair

import scala.collection.mutable

class NetworkPolicy extends DefaultAveragingPolicy[Double] {

  println("NetworkPolicy")

  val bytesSentStepToWorker = mutable.Map.empty[Int, mutable.Map[Int, Long]]

  override protected[this] def calculateAverage(step: Int, currentWorkers: Iterable[Int]): Option[Double] = {
    Some(ElasticPolicy.average(individualWorkerValues(step, currentWorkers).map(_._2)))
  }

  override protected[this] def individualWorkerValues(step: Int, currentWorkers: Iterable[Int]): Iterable[(Int, Double)] = {
    val workerBytesSent: Iterable[(Int, Double)] = currentWorkers.flatMap { workerId =>
      // get metrics per worker during a superstep
      val metrics = metricsOf(step, workerId, WorkerOperation.RUN_SUPERSTEP).get
      val bytesSentMetrics = metrics.flatMap {
        case Network(_,_,_, bytesSent) =>
          Some(bytesSent)
        case _ =>
          None
      }

      println(s"worker $workerId got  ${bytesSentMetrics.size} bytesSent entries")
      if (bytesSentMetrics.nonEmpty) {
        println(s"worker $workerId sent  ${bytesSentMetrics.max} bytes")
      } else {
        println(s"worker $workerId didn't have any updates during this step")
      }

      val prevValue = bytesSentStepToWorker
      .getOrElseUpdate(step - 1, mutable.Map.empty[Int, Long])
      .getOrElse(workerId, 0L)

      if (bytesSentMetrics.nonEmpty) {
        // by definition the current step has >= bytesSent iff it has metrics
        val max = bytesSentMetrics.max
        bytesSentStepToWorker
          .getOrElseUpdate(step, mutable.Map.empty[Int, Long])
          .put(workerId, max)
      } else {
        // it's empty so we should use prev step's value
        bytesSentStepToWorker
          .getOrElseUpdate(step, mutable.Map.empty[Int, Long])
          .put(workerId, prevValue)
      }

      val currentValue: Long = bytesSentStepToWorker
        .getOrElseUpdate(step, mutable.Map.empty[Int, Long])
        .getOrElse(workerId, 0L)

      val diff = currentValue - prevValue

      val result = Some(workerId -> diff.toDouble)
      println(s"$workerId current value: $currentValue prev value: $prevValue")
      result
    }

    if (workerBytesSent.isEmpty) {
      Iterable.empty
    } else {
      // we want to find the maximum worker traffic
      workerBytesSent
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
