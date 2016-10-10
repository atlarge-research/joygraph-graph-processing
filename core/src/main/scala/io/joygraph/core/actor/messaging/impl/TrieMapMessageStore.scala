package io.joygraph.core.actor.messaging.impl

import io.joygraph.core.actor.messaging.{Message, MessageStore, Messaging}

class TrieMapMessageStore extends MessageStore {
  private[this] val messaging : Messaging = new TrieMapMessaging

  def _handleMessage[I](index: WorkerId, dstMPair: Message[I], clazzI: Class[I], clazzM: Class[_]) {
    messaging.add(dstMPair.dst, dstMPair.msg)
  }

  def nextMessages[I,M](dst : I, clazzM : Class[M]) : Iterable[M] = {
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

  protected[messaging] def removeNextMessages[I](dst: I): Unit = {
    messaging.removeNext(dst)
  }
}
