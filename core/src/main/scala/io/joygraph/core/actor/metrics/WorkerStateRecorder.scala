package io.joygraph.core.actor.metrics

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class WorkerStateRecorder {
  private[this] val statesPerStep = TrieMap.empty[Int, scala.collection.mutable.Map[Int, scala.collection.mutable.Map[WorkerOperation.Value, WorkerState]]]
  private[this] val activeVerticesEndOfStep = TrieMap.empty[Int, scala.collection.mutable.Map[Int, Long]]

  def statesFor(step : Int, workerId : Int) = statesMap(step, workerId).toMap

  private def statesMap(step : Int, workerId : Int): mutable.Map[WorkerOperation.Value, WorkerState] = {
    val workerMap = statesPerStep.getOrElseUpdate(step, TrieMap.empty)
    workerMap.getOrElseUpdate(workerId, TrieMap.empty)
  }

  def setActiveVertices(step : Int, workerId : Int, num : Long) : Unit = {
    val workerMap = activeVerticesEndOfStep.getOrElseUpdate(step, TrieMap.empty)
    workerMap.getOrElseUpdate(workerId, num)
  }

  def activeVertices(step : Int, workerId : Int) : Option[Long] = {
    activeVerticesEndOfStep.get(step) match {
      case Some(x) =>
        x.get(workerId) match {
          case Some(y) => Some(y)
          case None => None
        }
      case None =>
        None
    }
  }

  def workerStateStart(step : Int, workerId : Int, state : WorkerOperation.Value, startTime : Long): Unit = {
    statesMap(step, workerId) += state -> WorkerState(state, startTime, None)
  }

  def workerStateStop(step : Int, workerId : Int, state : WorkerOperation.Value, stopTime : Long) : Unit = {
    statesMap(step, workerId)(state).stop = Some(stopTime)
  }
}
