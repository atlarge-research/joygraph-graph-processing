package io.joygraph.core.actor.elasticity

import io.joygraph.core.message.AddressPair

import scala.concurrent.{Future, Promise}
import scala.util.Try

class ElasticityPromise extends Promise[Map[Int, AddressPair]] {

  private[this] val defaultPromise : Promise[Map[Int, AddressPair]] = Promise[Map[Int, AddressPair]]

  override def future: Future[Map[Int, AddressPair]] = defaultPromise.future

  override def tryComplete(result: Try[Map[Int, AddressPair]]): Boolean = defaultPromise.tryComplete(result)

  override def isCompleted: Boolean = defaultPromise.isCompleted
}
