package io.joygraph.core.actor.messaging

import io.joygraph.core.util.SimplePool
import io.joygraph.core.util.collection.ReusableIterable

trait MessageStore {

  protected[this] def _handleMessage[I](index : Int, dstMPair : (I, _ <: Any), clazzI : Class[I], clazzM: Class[_ <: Any])
  protected[this] def messages[I,M](dst : I, clazzM : Class[M]) : Iterable[M]
  protected[this] def releaseMessages(messages : Iterable[_ <: Any], clazz : Class[_ <: Any])
  protected[this] def messagesOnSuperStepComplete()
  protected[this] def emptyCurrentMessages : Boolean

  // TODO move to a different interface
  /**
    * Pooling for serialized message iterables
    */
  protected[this] def setReusableIterablePool(pool : SimplePool[ReusableIterable[Any]]) : Unit = {}
}
