package io.joygraph.core.actor.messaging.impl

import io.joygraph.core.actor.messaging.{MessageStore, Messaging}

trait TrieMapMessageStore extends MessageStore {
  private[this] val messaging : Messaging = new TrieMapMessaging

  override protected[this] def _handleMessage[I,M](index : Int, dstMPair : (I, M), clazzI : Class[I], clazzM: Class[M]) {
  val (dst, m) = dstMPair
    messaging.add(dst, m)
  }

  override protected[this] def messages[I,M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    messaging.get(dst)
  }

  override protected[this] def messagesOnSuperStepComplete(): Unit = {
    messaging.onSuperStepComplete()
  }

  override protected[this] def emptyCurrentMessages : Boolean = {
    messaging.emptyCurrentMessages
  }

  protected[this] def releaseMessages[M](messages : Iterable[M], clazz : Class[M]) = {
    //noop
  }

}
