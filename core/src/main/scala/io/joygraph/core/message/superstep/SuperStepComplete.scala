package io.joygraph.core.message.superstep

import io.joygraph.core.program.Aggregator

case class SuperStepComplete(aggregators : Option[Map[String, Aggregator[_]]] = None) {

}
