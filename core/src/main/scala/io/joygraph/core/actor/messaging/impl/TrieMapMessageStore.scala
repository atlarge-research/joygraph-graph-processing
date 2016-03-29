package io.joygraph.core.actor.messaging.impl

import io.joygraph.core.actor.messaging.{MessageStore, Messaging}

trait TrieMapMessageStore extends MessageStore {
  private[this] val messaging : Messaging = new TrieMapMessaging

  override protected[this] def _handleMessage[I](index : Int, dstMPair : (I, _ <: Any), clazzI : Class[I], clazzM: Class[_ <: Any]) {
  val (dst, m) = dstMPair
    messaging.add(dst, m)
  }

  override protected[this] def nextMessages[I,M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    messaging.getNext(dst)
  }

  override protected[this] def messages[I,M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    messaging.get(dst)
  }

  override protected[this] def messagesOnBarrier(): Unit = {
    messaging.onBarrier()
  }

  override protected[this] def emptyNextMessages : Boolean = {
    messaging.emptyNextMessages
  }

  protected[this] def releaseMessages(messages : Iterable[_ <: Any], clazz : Class[_ <: Any]) = {
    //noop
  }

  override protected[this] def removeMessages[I](dst : I): Unit = {
    messaging.remove(dst)
  }

}
