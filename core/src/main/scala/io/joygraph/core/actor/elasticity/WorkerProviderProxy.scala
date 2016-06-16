package io.joygraph.core.actor.elasticity

import akka.actor.ActorRef
import com.typesafe.config.Config
import io.joygraph.core.message.elasticity.{WorkersRequest, WorkersResponse}

import scala.concurrent.{Future, Promise}

class WorkerProviderProxy(workerProviderActorRef : ActorRef) {

  private[this] var currentPromise : Promise[WorkersResponse] = _

  def requestWorkers(jobConf : Config, numWorkers : Int) : Future[WorkersResponse] = {
    currentPromise = Promise[WorkersResponse]
    workerProviderActorRef ! WorkersRequest(jobConf, numWorkers)
    currentPromise.future
  }

  def response(response : WorkersResponse) = {
    currentPromise.success(response)
  }
}
