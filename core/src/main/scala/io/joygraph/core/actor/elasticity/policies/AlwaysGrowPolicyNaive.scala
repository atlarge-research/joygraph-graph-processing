package io.joygraph.core.actor.elasticity.policies

import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Result}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.VertexHashPartitioner

class AlwaysGrowPolicyNaive extends ElasticPolicy {

  override def init(policyParams: Config): Unit = {

  }

  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
    // repartition all according to deviation
    val newWorkerId = currentWorkers.keys.max + 1

    Some(Grow(Array(newWorkerId), new VertexHashPartitioner(currentWorkers.size + 1)))
  }
}
