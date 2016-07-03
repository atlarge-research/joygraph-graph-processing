package io.joygraph.core.actor.elasticity.policies
import akka.cluster.metrics.StandardMetrics
import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Result}
import io.joygraph.core.actor.metrics.WorkerOperation
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.ReHashPartitioner

class CPUPolicy extends ElasticPolicy {

  private[this] var c : Double = _
  private[this] var d : Double = _

  override def init(policyParams: Config): Unit = {
    c = policyParams.getDouble("c")
    d = policyParams.getDouble("d")
  }

  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
    // get average CPU over all workers during the running of the step
    val workerLoadAverages: Iterable[(Int, Double)] = currentWorkers.keys.map {
      workerId =>
        // get processing time
        val metrics = metricsOf(currentStep, workerId, WorkerOperation.RUN_SUPERSTEP).get
        workerId ->
          ElasticPolicy.singleWorkerAverageOption[Double](metrics,
            StandardMetrics.extractCpu(_).systemLoadAverage, _.sum)
    }

    val globalAverage : Double = ElasticPolicy.average(workerLoadAverages.map(_._2))
    val standardDeviation : Double = ElasticPolicy.standardDeviation(workerLoadAverages.map(_._2), Some(globalAverage))
    val workersDeviations = workerLoadAverages.map{case (id, loadAverage) => id -> math.abs(loadAverage - globalAverage)}

    val (workerId, maximumDeviation) = workersDeviations.maxBy(_._2)
    if (maximumDeviation > c * standardDeviation) {
      val newWorkerId = currentWorkers.keys.max + 1
      val newPartitionerBuilder = new ReHashPartitioner.Builder
      newPartitionerBuilder.distribute(workerId, (Array[Int](newWorkerId), 0.5))
      newPartitionerBuilder.partitioner(currentPartitioner)
      Some(Grow(Iterable(newWorkerId), newPartitionerBuilder.build()))
    } else {
      None
    }
  }
}
