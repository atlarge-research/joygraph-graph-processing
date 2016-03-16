package io.joygraph.cluster.submission

import akka.actor.{Actor, ActorLogging}

abstract class SubmissionClient extends Actor with ActorLogging {

  protected[this] def uploadJar(srcPath: String, dstPath: String)

  def submit(): Unit = {
  }

  override def receive: Receive = ???
}