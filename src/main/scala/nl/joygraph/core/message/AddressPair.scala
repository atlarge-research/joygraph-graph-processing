package nl.joygraph.core.message

import akka.actor.ActorRef

case class AddressPair(actorRef : ActorRef, nettyAddress : NettyAddress) {

}
