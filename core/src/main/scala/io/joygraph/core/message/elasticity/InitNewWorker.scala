package io.joygraph.core.message.elasticity

import akka.actor.ActorRef

case class InitNewWorker(masterActorRef : ActorRef, currentSuperStep : Int, numVertices : Long, numEdges : Long) {

}
