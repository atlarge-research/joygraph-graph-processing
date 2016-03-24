package io.joygraph.core.util.serde

import java.nio.ByteBuffer
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import io.joygraph.core.program.Combinable
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.buffers.streams.CombinerBuffer
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class CombinableAsyncSerializer[I](msgType : Byte, n : Int, kryoFactory : => Kryo, idSerializer : (Kryo, KryoOutput, I) => Any, val bufferExceededThreshold : Int = 1 * 1024 * 1024) {

  private[this] val combinerBuffers : ArrayBuffer[CombinerBuffer[I]] = ArrayBuffer.fill(n)(new CombinerBuffer[I](bufferExceededThreshold, idSerializer))
  private[this] val _buffers : ArrayBuffer[ObjectByteBufferOutputStream] = ArrayBuffer.fill(n)(new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold))
  private[this] val bufferSwaps : ArrayBuffer[BlockingQueue[ObjectByteBufferOutputStream]] = ArrayBuffer.fill(n)(new LinkedBlockingQueue[ObjectByteBufferOutputStream])
  private[this] val kryos : ArrayBuffer[Kryo] = ArrayBuffer.fill(n)(kryoFactory)
  private[this] val kryoOutputs : ArrayBuffer[KryoOutput] = ArrayBuffer.fill(n)(new KryoOutput(4096, 4096))
  private[this] val byteBufferInputs : ArrayBuffer[ByteBufferInput] = ArrayBuffer.fill(n)(new ByteBufferInput(4096))
  private[this] val locks : ArrayBuffer[Object] = ArrayBuffer.fill(n)(new Object)

  // initialize bufferSwap
  bufferSwaps.foreach(_.add(new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold)))

  def serialize[M](index : Int, i: I, m : M, messageSerializer : (Kryo, KryoOutput, M) => Any, messageDeserializer : (Kryo, ByteBufferInput) => M)(outputHandler : ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext, combinable : Combinable[M]) : Unit = {
    locks(index).synchronized {
      val combinerBuffer = combinerBuffers(index)
      implicit val kryo = kryos(index)
      implicit val kryoOutput = kryoOutputs(index)
      implicit val byteBufferInput = byteBufferInputs(index)
      if (!combinerBuffer.add(i,m, combinable.combine, messageSerializer, messageDeserializer)) {
        val buffer = _buffers(index)
        // fill the byteBufferInputs
        combinerBuffer.asByteBuffers().foreach {
          bb =>
            bb.flip()
            buffer.write(bb)
            buffer.increment()
        }
        // empty combinerBuffer
        combinerBuffer.clear()

        buffer.writeCounter()
        // switch send buffer
        val bufferSwap = bufferSwaps(index)
        val osSwap = bufferSwap.take()
        _buffers(index) = osSwap

        outputHandler(buffer.handOff()).foreach { _ =>
          // reset the osSwap, it's been used and needs to be set to pos 0
          buffer.resetOOS()
          // buffers in bufferswap are now clean!
          bufferSwap.add(buffer)
        } // sweet I got it back

        assert(combinerBuffer.add(i,m, combinable.combine, messageSerializer, messageDeserializer))
      }
    }
  }

  def sendNonEmptyByteBuffers(outputHandler : ((ByteBuffer, Int)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext) : Unit = combinerBuffers.zipWithIndex.filter(_._1.size() > 0).foreach {
    case (combinerBuffer, b) =>
      val buffer = _buffers(b)
      combinerBuffer.asByteBuffers().foreach {
        bb =>
          bb.flip()
          buffer.write(bb)
          buffer.increment()
      }
      combinerBuffer.clear()

      buffer.writeCounter()
      outputHandler((buffer.handOff(), b)).foreach(_ => _buffers(b).resetOOS())
  }
}