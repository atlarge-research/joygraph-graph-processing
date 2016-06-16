package io.joygraph.core.message.elasticity

import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner

case class ElasticDistribute(prevWorkers : Map[Int, AddressPair], nextWorkers : Map[Int, AddressPair], vPartitioner : VertexPartitioner)