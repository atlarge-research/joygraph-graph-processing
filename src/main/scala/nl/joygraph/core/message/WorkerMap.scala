package nl.joygraph.core.message

import akka.actor.ActorRef

import scala.collection.mutable.ArrayBuffer

case class WorkerMap(val workers : ArrayBuffer[ActorRef])