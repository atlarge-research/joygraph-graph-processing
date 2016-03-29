package io.joygraph.core.actor

import akka.actor.{Actor, ActorLogging}
import com.typesafe.config.Config
import io.joygraph.core.message.elasticity.{WorkersRequest, WorkersResponse}

import scala.concurrent.Future

abstract class WorkerProvider extends Actor with ActorLogging {
  import scala.concurrent.ExecutionContext.Implicits.global

  def response(jobConf : Config, numWorkers : Int): Future[WorkersResponse]

  override def receive: Receive = {
    case WorkersRequest(jobConf, numWorkers) =>
      val sendRef = sender()
      response(jobConf, numWorkers).foreach(
        sendRef ! _
      )
  }
}
