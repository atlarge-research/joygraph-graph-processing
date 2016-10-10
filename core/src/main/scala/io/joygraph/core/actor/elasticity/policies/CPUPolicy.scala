package io.joygraph.core.actor.elasticity.policies
import akka.cluster.metrics.StandardMetrics.Cpu
import io.joygraph.core.actor.metrics.WorkerOperation
import io.joygraph.core.message.AddressPair

class CPUPolicy extends DefaultAveragingPolicy[Double] {

  println("CPUPolicy")

  //  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
//    // get average CPU over all workers during the running of the step
//    val workerLoadAverages: Iterable[(Int, Double)] = currentWorkers.keys.map {
//      workerId =>
//        // get processing time
//        val metrics = metricsOf(currentStep, workerId, WorkerOperation.RUN_SUPERSTEP).get
//        workerId ->
//          ElasticPolicy.singleWorkerAverageOption[Double](metrics,
//            StandardMetrics.extractCpu(_).systemLoadAverage, _.sum)
//    }
//
//    val globalAverage : Double = ElasticPolicy.average(workerLoadAverages.map(_._2))
//    val standardDeviation : Double = ElasticPolicy.standardDeviation(workerLoadAverages.map(_._2), Some(globalAverage))
//    val workersDeviations = workerLoadAverages.map{case (id, loadAverage) => id -> math.abs(loadAverage - globalAverage)}
//
//    val (workerId, maximumDeviation) = workersDeviations.maxBy(_._2)
//    if (maximumDeviation > c * standardDeviation) {
//      val newWorkerId = currentWorkers.keys.max + 1
//      val newPartitionerBuilder = new ReHashPartitioner.Builder
//      newPartitionerBuilder.distribute(workerId, (Array[Int](newWorkerId), 0.5))
//      newPartitionerBuilder.partitioner(currentPartitioner)
//      Some(Grow(Iterable(newWorkerId), newPartitionerBuilder.build()))
//    } else {
//      None
//    }
//  }


  /**
    * We can only make a decision if we have metric data for each worker available
    */
  override def enoughInformationToMakeDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Boolean = {
    individualWorkerValues(currentStep, currentWorkers.keys).size == currentWorkers.size
  }

  override protected[this] def calculateAverage(step: Int, currentWorkers: Iterable[Int]): Option[Double] = {
    Some(ElasticPolicy.average(individualWorkerValues(step, currentWorkers).map(_._2)))
  }

  override protected[this] def individualWorkerValues(step: Int, currentWorkers: Iterable[Int]): Iterable[(Int, Double)] = {
    // get average CPU over all workers during the running of the step
    currentWorkers.flatMap { workerId =>
      // get processing time
      val metrics = metricsOf(step, workerId, WorkerOperation.RUN_SUPERSTEP).get
      val values = metrics.flatMap{
        case Cpu(address, timestamp, systemLoadAverage, cpuCombined, cpuStolen, processors) =>
          systemLoadAverage
      }

      if (values.isEmpty) {
        None
      } else {
        Some(workerId -> ElasticPolicy.average(values))
      }
    }
  }

  /**
    * Returns 1, if it has a positive impact given the evaluation function, negative impact -1 and there was no decision 0
    */
  override protected[this] def evaluatePreviousDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Int = {
    // check if decision exists
    decision(previousStep(currentStep)) match {
      case Some(x) =>
        if (stepAverage(currentStep).get >= stepAverage(previousStep(currentStep)).get) {
          -1
        } else {
          1
        }
      case None =>
        0
    }
  }
}
