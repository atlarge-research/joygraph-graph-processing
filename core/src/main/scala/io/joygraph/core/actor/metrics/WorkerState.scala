package io.joygraph.core.actor.metrics

object WorkerOperation extends Enumeration {
  val LOAD_DATA, PREPARE_FIRST_SUPERSTEP, BARRIER, RUN_SUPERSTEP, DISTRIBUTE_DATA = Value
}

case class WorkerState(operation : WorkerOperation.Value, start : Long, var stop : Option[Long])
