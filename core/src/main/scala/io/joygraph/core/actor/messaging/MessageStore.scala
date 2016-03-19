package io.joygraph.core.actor.messaging

import io.joygraph.core.util.SimplePool
import io.joygraph.core.util.collection.ReusableIterable

trait MessageStore {

  protected[this] def _handleMessage[I,M](index : Int, dstMPair : (I, M), clazzI : Class[I], clazzM: Class[M])
  protected[this] def messages[I,M](dst : I, clazzM : Class[M]) : Iterable[M]
  protected[this] def releaseMessages[M](messages : Iterable[M], clazz : Class[M])
  protected[this] def messagesOnSuperStepComplete()
  protected[this] def emptyCurrentMessages : Boolean

  // TODO move to a different interface
  /**
    * Pooling for serialized message iterables
    */
  protected[this] def addPool(clazz : Class[_], pool : SimplePool[ReusableIterable[Any]]) : Unit = {}
}
