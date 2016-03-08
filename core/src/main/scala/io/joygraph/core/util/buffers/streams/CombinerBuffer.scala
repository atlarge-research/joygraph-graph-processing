package io.joygraph.core.util.buffers.streams

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import io.joygraph.core.util.DirectByteBufferGrowingOutputStream
import io.joygraph.core.util.buffers.KryoOutput
import io.netty.util.internal.PlatformDependent

import scala.collection.concurrent.TrieMap

/**
  * Buffer which counts the bytes that will eventually be sent through the network.
  */
class CombinerBuffer[I, M](maxSizeBytes : Int, combiner : (M, M) => M, idSerializer : (Kryo, KryoOutput, I) => Any, serializer : (Kryo, KryoOutput, M) => Any, deserializer : (Kryo, ByteBufferInput) => M) {

  private[this] val buffer : TrieMap[I, CombineOutputStream] = TrieMap.empty[I, CombineOutputStream]
  private[this] var currentSize = 0

  private[this] def get(i : I) : CombineOutputStream = {
    buffer.getOrElseUpdate(i, new CombineOutputStream)
  }

  def asByteBuffers(): Iterator[ByteBuffer] = {
    buffer.valuesIterator.filter(_.messageSize() > 0).map(_.getBuf)
  }

  /**
    * @return true on success, false when buffer is full.
    */
  def add(i : I, m : M)(implicit bbInput : ByteBufferInput, kryoOutput : KryoOutput, kryo : Kryo): Boolean = {
    val os = get(i)

    val newM = if (os.idSize() > 0) {
      val bb = os.getBuf
      bb.flip()
      currentSize -= os.messageSize()
      bbInput.setBuffer(bb)
      // combine
      combiner(deserializer(kryo, bbInput), m)
    } else {
      kryoOutput.setOutputStream(os)
      idSerializer(kryo, kryoOutput, i)
      if (currentSize + kryoOutput.total() > maxSizeBytes ) {
        return false
      } else {
        kryoOutput.flush() // the first write
      }
      m
    }

    kryoOutput.setOutputStream(os)
    serializer(kryo, kryoOutput, newM)
    if ((currentSize + os.idSize() + kryoOutput.total()) > maxSizeBytes) {
      currentSize += os.messageSize()
      false
    } else {
      kryoOutput.flush()
      os.trim()
      currentSize += os.messageSize()
      true
    }
  }

  def size() : Int = {
    currentSize
  }

  def clear() = {
    currentSize = 0
    // delete buffers
    buffer.valuesIterator.foreach(x => PlatformDependent.freeDirectBuffer(x.getBuf))
    // clear map
    buffer.clear()
  }

  /**
    * After writing the id, it does not overwrite it anymore, the combined message will be always after it.
    */
  private class CombineOutputStream extends DirectByteBufferGrowingOutputStream(0) {
    private[this] var firstWrite = true
    private[this] var resetPosition = 0

    override def write(byteBuffer : ByteBuffer) {
      if (firstWrite) {
        super.write(byteBuffer)
        resetPosition = byteBuffer.position()
        firstWrite = false
      } else {
        buf.position(resetPosition)
        super.write(byteBuffer)
      }
    }

    def messageSize() = buf.duplicate().flip().remaining() - resetPosition

    def idSize() = resetPosition

  }
}
