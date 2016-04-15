package io.joygraph.core.actor.messaging.impl

import io.joygraph.core.actor.messaging.{MessageStore, Messaging}

class TrieMapMessageStore extends MessageStore {
  private[this] val messaging : Messaging = new TrieMapMessaging

  def _handleMessage[I](index : Int, dstMPair : (I, _ <: Any), clazzI : Class[I], clazzM: Class[_ <: Any]) {
  val (dst, m) = dstMPair
    messaging.add(dst, m)
  }

  protected[messaging] def nextMessages[I,M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    messaging.getNext(dst)
  }

  def messages[I,M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    messaging.get(dst)
  }

  def messagesOnBarrier(): Unit = {
    messaging.onBarrier()
  }

  def emptyNextMessages : Boolean = {
    messaging.emptyNextMessages
  }

  def releaseMessages(messages : Iterable[_ <: Any], clazz : Class[_ <: Any]) = {
    //noop
  }

  protected[messaging] def removeMessages[I](dst : I): Unit = {
    messaging.remove(dst)
  }

}
