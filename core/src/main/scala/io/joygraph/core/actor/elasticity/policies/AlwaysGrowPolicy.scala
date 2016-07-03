package io.joygraph.core.actor.elasticity.policies

import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Result}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.ReHashPartitioner

class AlwaysGrowPolicy extends ElasticPolicy {

  override def init(policyParams: Config): Unit = {

  }

  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
    // repartition all according to deviation
    val newWorkerId = currentWorkers.keys.max + 1
    val newPartitionerBuilder = new ReHashPartitioner.Builder
    currentWorkers.foreach {
      case (workerId, _) =>
        newPartitionerBuilder.distribute(workerId, (Array(newWorkerId), 1.0 / currentWorkers.size.toDouble))
    }

    newPartitionerBuilder.partitioner(currentPartitioner)
    Some(Grow(Array(newWorkerId), newPartitionerBuilder.build()))
  }
}
