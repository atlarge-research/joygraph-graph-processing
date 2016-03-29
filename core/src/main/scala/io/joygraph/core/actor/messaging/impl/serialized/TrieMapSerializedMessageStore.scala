package io.joygraph.core.actor.messaging.impl.serialized

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.pool.{KryoFactory, KryoPool}
import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.{KryoSerialization, SimplePool}

trait TrieMapSerializedMessageStore extends MessageStore with KryoSerialization {

  private[this] val messaging = new TrieMapSerializedMessaging
  protected[this] var partitioner : VertexPartitioner
  private[this] var _pool : SimplePool[ReusableIterable[Any]] = _
  // TODO inject factory from worker
  private[this] val kryoPool = new KryoPool.Builder(new KryoFactory {
    override def create(): Kryo = new Kryo()
  }).build()

  override protected[this] def setReusableIterablePool(pool : SimplePool[ReusableIterable[Any]]) : Unit = {
    _pool = pool
  }
  private def reusableIterablePool(): SimplePool[ReusableIterable[Any]] = _pool

  override protected[this] def _handleMessage[I](index : Int, dstMPair : (I, _ <: Any), clazzI : Class[I], clazzM: Class[_ <: Any]) {
    implicit val kryoInstance = kryo(index)
    kryoInstance.synchronized {
      implicit val kryoOutputInstance = kryoOutput(index)
      val (dst, m) = dstMPair
      messaging.add(dst, m)
    }
  }

  override protected[this] def removeMessages[I](dst : I): Unit = {
    messaging.remove(dst)
  }

  override protected[this] def nextMessages[I,M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    // todo remove code duplication with messages
    implicit val iterable = reusableIterablePool().borrow()
    iterable.kryo(kryoPool.borrow())
    val messages = messaging.getNext(dst)
    if(messages.isEmpty) {
      releaseMessages(iterable, clazzM)
    }
    messages.asInstanceOf[Iterable[M]]
  }

  override protected[this] def messages[I, M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    implicit val iterable = reusableIterablePool().borrow()
    iterable.kryo(kryoPool.borrow())
    val messages = messaging.get(dst)
    if(messages.isEmpty) {
      releaseMessages(iterable, clazzM)
    }
    messages.asInstanceOf[Iterable[M]]
  }

  override protected[this] def releaseMessages(messages : Iterable[_ <: Any], clazz : Class[_ <: Any]) = {
    messages match {
      case reusableIterable : ReusableIterable[Any] =>
        kryoPool.release(reusableIterable.kryo)
        reusableIterablePool().release(reusableIterable)
      case _ => // noop
    }
  }

  override protected[this] def messagesOnBarrier() = {
    messaging.onBarrier()
  }

  override protected[this] def emptyNextMessages : Boolean = {
    messaging.emptyNextMessages
  }
}
