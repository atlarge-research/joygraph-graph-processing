package io.joygraph.core.message.elasticity

import io.joygraph.core.message.AddressPair

case class ElasticRemoval(workers : Map[Int, AddressPair])