package io.joygraph.core.actor.messaging.impl.serialized

import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.{KryoSerialization, SimplePool}

import scala.collection.mutable

trait TrieMapSerializedMessageStore extends MessageStore with KryoSerialization {

  private[this] val messaging = new TrieMapSerializedMessaging
  protected[this] var partitioner : VertexPartitioner
  private[this] val pools = mutable.OpenHashMap.empty[Class[_], SimplePool[ReusableIterable[Any]]]

  override protected[this] def addPool(clazz : Class[_], pool : SimplePool[ReusableIterable[Any]]) : Unit = {
    pools += clazz -> pool
  }
  private[this] def reusableIterablePool[M](clazz : Class[M]) : SimplePool[ReusableIterable[M]] = pools(clazz).asInstanceOf[SimplePool[ReusableIterable[M]]]

  override protected[this] def _handleMessage[I,M](index : Int, dstMPair : (I, M), clazzI : Class[I], clazzM: Class[M]) {
    implicit val kryoInstance = kryo(index)
    kryoInstance.synchronized {
      implicit val kryoOutputInstance = kryoOutput(index)
      val (dst, m) = dstMPair
      messaging.add(dst, m)
    }
  }

  override protected[this] def messages[I, M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    val index = partitioner.destination(dst)
    implicit val iterable = reusableIterablePool(clazzM).borrow()
    iterable.kryo(kryo(index))
    val messages = messaging.get(dst)
    if(messages.isEmpty) {
      releaseMessages(iterable, clazzM)
    }
    messages
  }

  override protected[this] def releaseMessages[M](messages : Iterable[M], clazz : Class[M]) = {
    messages match {
      case reusableIterable : ReusableIterable[M] =>
        reusableIterablePool(clazz).release(reusableIterable)
      case _ => // noop
    }
  }

  override protected[this] def messagesOnSuperStepComplete() = {
    messaging.onSuperStepComplete()
  }

  override protected[this] def emptyCurrentMessages : Boolean = {
    messaging.emptyCurrentMessages
  }
}
