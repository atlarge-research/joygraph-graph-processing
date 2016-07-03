package io.joygraph.core.actor.elasticity.policies

import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Result, Shrink}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.VertexHashPartitioner

class AlwaysShrinkPolicyNaive extends ElasticPolicy {

  override def init(policyParams: Config): Unit = {

  }

  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
    // repartition all according to deviation
    Some(Shrink(Array(currentWorkers.keys.max), new VertexHashPartitioner(currentWorkers.size - 1)))
  }
}
