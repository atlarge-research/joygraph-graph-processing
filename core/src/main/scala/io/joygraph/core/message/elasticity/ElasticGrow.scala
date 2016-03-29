package io.joygraph.core.message.elasticity

import io.joygraph.core.message.AddressPair

case class ElasticGrow(prevWorkers : Map[Int, AddressPair], nextWorkers : Map[Int, AddressPair]) {

}
