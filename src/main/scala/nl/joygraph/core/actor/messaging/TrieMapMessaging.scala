package nl.joygraph.core.actor.messaging

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap

class TrieMapMessaging[I,M] extends Messaging[I,M] {

  private[this] var nextMessages = TrieMap.empty[I, ConcurrentLinkedQueue[M]]
  private[this] var currentMessages = TrieMap.empty[I, ConcurrentLinkedQueue[M]]

  override def onSuperStepComplete(): Unit = {
    currentMessages = nextMessages
    nextMessages = TrieMap.empty[I, ConcurrentLinkedQueue[M]]
  }

  /**
    * Retrieve messages for source if any
    */
  override def get(source: I): Iterable[M] = {
    currentMessages.get(source) match {
      case Some(x) => x.toIterable
      case None => Iterable.empty[M]
    }
  }

  /**
    * Add message to source
    */
  override def add(source: I, message: M): Unit = {
    nextMessages.getOrElseUpdate(source, new ConcurrentLinkedQueue[M]()).add(message)
  }

  override def emptyCurrentMessages: Boolean = currentMessages.isEmpty
}
