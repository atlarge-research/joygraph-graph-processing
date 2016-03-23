package io.joygraph.core.actor.messaging.impl.serialized

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.actor.messaging.SerializedMessaging
import io.joygraph.core.util.DirectByteBufferGrowingOutputStream
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable

import scala.collection.concurrent.TrieMap

class TrieMapSerializedMessaging extends SerializedMessaging {

  private[this] var nextMessages = TrieMap.empty[Any, DirectByteBufferGrowingOutputStream]
  private[this] var currentMessages = TrieMap.empty[Any, DirectByteBufferGrowingOutputStream]

  override def onSuperStepComplete(): Unit = {
    // TODO new TrieMaps are constructed which creates a lot of garbage
    currentMessages = nextMessages
    nextMessages = TrieMap.empty[Any, DirectByteBufferGrowingOutputStream]
  }

  /**
    * Retrieve messages for source if any
    */
  override def get[I,M](source: I)(implicit reusableIterable: ReusableIterable[M]): Iterable[M] = {
    currentMessages.get(source) match {
      case Some(os) =>
        reusableIterable.bufferProvider(() => os.getBuf)
      case None => EMPTY_MESSAGES.asInstanceOf[Iterable[M]]
    }
  }

  /**
    * Add message to source
    */
  override def add[I,M](source: I, message: M)(implicit kryo: Kryo, output : KryoOutput): Unit = {
    val outputStream = nextMessages.getOrElseUpdate(source, new DirectByteBufferGrowingOutputStream(8))
    output.setOutputStream(outputStream)
    kryo.writeObject(output, message)
    output.flush()
    // TODO evaluate the removal of trim
//    outputStream.trim()
  }

  override def emptyCurrentMessages: Boolean = currentMessages.isEmpty
}
