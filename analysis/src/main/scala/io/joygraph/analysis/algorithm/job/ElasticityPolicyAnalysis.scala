package io.joygraph.analysis.algorithm.job

import io.joygraph.analysis.ElasticPolicyReader
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.impl.VertexHashPartitioner

trait ElasticityPolicyAnalysis {
  val filePath : String
  val policy : ElasticPolicy
  policy.importMetrics(filePath)

  def currentWorkersForStep(step : Int) : Map[Int, AddressPair] = {
    val supplyDemandMetrics = policy.rawSupplyDemands.filter(x => x.superStep == step && x.supply == x.demand)(0)
    val numWorkers = supplyDemandMetrics.demand
    (for (i <- 0 until numWorkers) yield {
      i -> AddressPair(null, null)
    }).toMap
  }

  def simulate(step : Int) : Option[ElasticPolicy.Result] = {

    policy.decide(step, currentWorkersForStep(step), new VertexHashPartitioner(), 20)
  }

}
