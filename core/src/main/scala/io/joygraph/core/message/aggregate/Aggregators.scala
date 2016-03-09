package io.joygraph.core.message.aggregate

import io.joygraph.core.program.Aggregator

case class Aggregators(aggregators : Map[String, Aggregator[_]]) {

}
