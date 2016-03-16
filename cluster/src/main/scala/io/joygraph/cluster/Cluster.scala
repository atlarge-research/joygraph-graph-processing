package io.joygraph.cluster

import akka.actor.Actor

class Cluster extends Actor {
  override def receive: Receive = {
    case jobSubmission @ JobSubmission() =>
  }
}
