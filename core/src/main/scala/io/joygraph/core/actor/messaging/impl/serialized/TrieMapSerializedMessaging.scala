package io.joygraph.core.actor.messaging.impl.serialized

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.util.DirectByteBufferGrowingOutputStream
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable

import scala.collection.concurrent.TrieMap

class TrieMapSerializedMessaging[I,M] extends SerializedMessaging[I,M] {

  private[this] var nextMessages = TrieMap.empty[I, DirectByteBufferGrowingOutputStream]
  private[this] var currentMessages = TrieMap.empty[I, DirectByteBufferGrowingOutputStream]

  override def onSuperStepComplete(): Unit = {
    currentMessages = nextMessages
    nextMessages = TrieMap.empty[I, DirectByteBufferGrowingOutputStream]
  }

  /**
    * Retrieve messages for source if any
    */
  override def get(source: I)(implicit reusableIterable: ReusableIterable[M]): Iterable[M] = {
    currentMessages.get(source) match {
      case Some(os) =>
        reusableIterable.bufferProvider(() => os.getBuf)
      case None => EMPTY_MESSAGES
    }
  }

  /**
    * Add message to source
    */
  override def add(source: I, message: M)(implicit kryo: Kryo, output : KryoOutput): Unit = {
    val outputStream = nextMessages.getOrElseUpdate(source, new DirectByteBufferGrowingOutputStream(8))
    output.setOutputStream(outputStream)
    kryo.writeObject(output, message)
    output.flush()
    outputStream.trim()
  }

  override def emptyCurrentMessages: Boolean = currentMessages.isEmpty
}