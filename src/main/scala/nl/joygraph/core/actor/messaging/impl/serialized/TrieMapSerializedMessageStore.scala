package nl.joygraph.core.actor.messaging.impl.serialized

import com.esotericsoftware.kryo.io.ByteBufferInput
import nl.joygraph.core.actor.messaging.MessageStore
import nl.joygraph.core.partitioning.VertexPartitioner
import nl.joygraph.core.util.collection.ReusableIterable
import nl.joygraph.core.util.{KryoSerialization, SimplePool}

trait TrieMapSerializedMessageStore[I,M] extends MessageStore[I,M] with KryoSerialization {

  private[this] val messaging = new TrieMapSerializedMessaging[I,M]
  protected[this] var partitioner : VertexPartitioner
  protected[this] val clazzM : Class[M]
  private[this] val reusableIterablePool = new SimplePool[ReusableIterable[M]]({
    val reusableIterable = new ReusableIterable[M] {
      override protected[this] def deserializeObject(): M = _kryo.readObject(_input, clazzM)
    }
    reusableIterable.input(new ByteBufferInput(maxMessageSize))
    reusableIterable
  })


  override protected[this] def _handleMessage(index : Int, dstMPair : (I, M)): Unit = {
    implicit val kryoInstance = kryo(index)
    kryoInstance.synchronized {
      implicit val kryoOutputInstance = kryoOutput(index)
      val (dst, m) = dstMPair
      messaging.add(dst, m)
    }
  }

  override protected[this] def messages(dst : I) : Iterable[M] = {
    val index = partitioner.destination(dst)
    implicit val iterable = reusableIterablePool.borrow()
    iterable.kryo(kryo(index))
    val messages = messaging.get(dst)
    if(messages.isEmpty) {
      releaseMessages(iterable)
    }
    messages
  }

  override protected[this] def releaseMessages(messages : Iterable[M]) = {
    messages match {
      case reusableIterable : ReusableIterable[M] =>
        reusableIterablePool.release(reusableIterable)
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
