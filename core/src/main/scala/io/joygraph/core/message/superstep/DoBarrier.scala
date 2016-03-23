package io.joygraph.core.message.superstep

import io.joygraph.core.program.Aggregator

case class DoBarrier(superStep : Int, aggregatorMapping : Map[String, Aggregator[_]]) {

}
