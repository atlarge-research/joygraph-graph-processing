package io.joygraph.core.actor.messaging.impl

import java.util.concurrent.ConcurrentLinkedQueue

import io.joygraph.core.actor.messaging.Messaging

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap

class TrieMapMessaging extends Messaging {

  private[this] var nextMessages = TrieMap.empty[Any, ConcurrentLinkedQueue[Any]]
  private[this] var currentMessages = TrieMap.empty[Any, ConcurrentLinkedQueue[Any]]

  override def onSuperStepComplete(): Unit = {
    currentMessages = nextMessages
    nextMessages = TrieMap.empty[Any, ConcurrentLinkedQueue[Any]]
  }

  /**
    * Retrieve messages for source if any
    */
  override def get[I,M](source : I) : Iterable[M] = {
    currentMessages.get(source) match {
      case Some(x) => x.toIterable.asInstanceOf[Iterable[M]]
      case None => EMPTY_MESSAGES.asInstanceOf[Iterable[M]]
    }
  }

  /**
    * Add message to source
    */
  override def add[I,M](source: I, message : M): Unit = {
    nextMessages.getOrElseUpdate(source, new ConcurrentLinkedQueue[Any]()).add(message)
  }

  override def emptyCurrentMessages: Boolean = currentMessages.isEmpty
}
