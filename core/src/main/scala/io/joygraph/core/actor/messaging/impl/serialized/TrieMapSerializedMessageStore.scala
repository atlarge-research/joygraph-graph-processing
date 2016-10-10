package io.joygraph.core.actor.messaging.impl.serialized

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.pool.{KryoFactory, KryoPool}
import io.joygraph.core.actor.messaging.{Message, MessageStore}
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.{KryoSerialization, SimplePool}

class TrieMapSerializedMessageStore(protected[this] var partitioner : VertexPartitioner, maxMessageSize : Int) extends MessageStore with KryoSerialization {

  private[this] val messaging = new TrieMapSerializedMessaging
  private[this] var _pool : SimplePool[ReusableIterable[Any]] = _
  // TODO inject factory from worker
  private[this] val kryoPool = new KryoPool.Builder(new KryoFactory {
    override def create(): Kryo = new Kryo()
  }).build()

  override def setReusableIterableFactory(factory: => ReusableIterable[Any]): Unit = {
    _pool = new SimplePool[ReusableIterable[Any]](factory)
  }
  private def reusableIterablePool(): SimplePool[ReusableIterable[Any]] = _pool

  def _handleMessage[I](index: WorkerId, dstMPair: Message[I], clazzI: Class[I], clazzM: Class[_]) {
    implicit val kryoInstance = kryo(index)
    kryoInstance.synchronized { // TODO remove after conversion to class
      implicit val kryoOutputInstance = kryoOutput(index)
      messaging.add(dstMPair.dst, dstMPair.msg)
    }
  }

  protected[messaging] def removeMessages[I](dst : I): Unit = {
    messaging.remove(dst)
  }

  def nextMessages[I,M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    // todo remove code duplication with messages
    implicit val iterable = reusableIterablePool().borrow()
    iterable.kryo(kryoPool.borrow())
    val messages = messaging.getNext(dst)
    if(messages.isEmpty) {
      releaseMessages(iterable, clazzM)
    }
    messages.asInstanceOf[Iterable[M]]
  }

  def messages[I, M](dst : I, clazzM : Class[M]) : Iterable[M] = {
    implicit val iterable = reusableIterablePool().borrow()
    iterable.kryo(kryoPool.borrow())
    val messages = messaging.get(dst)
    if(messages.isEmpty) {
      releaseMessages(iterable, clazzM)
    }
    messages.asInstanceOf[Iterable[M]]
  }

  def releaseMessages(messages : Iterable[_ <: Any], clazz : Class[_ <: Any]) = {
    messages match {
      case reusableIterable : ReusableIterable[Any] =>
        kryoPool.release(reusableIterable.kryo)
        reusableIterablePool().release(reusableIterable)
      case _ => // noop
    }
  }

  def messagesOnBarrier() = {
    messaging.onBarrier()
  }

  def emptyNextMessages : Boolean = {
    messaging.emptyNextMessages
  }

  override protected[messaging] def removeNextMessages[I](dst: I): Unit = messaging.removeNext(dst)

  override protected[this] def kryoOutputFactory: KryoOutput = new KryoOutput(maxMessageSize, maxMessageSize)

}
