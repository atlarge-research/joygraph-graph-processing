package io.joygraph.core.util.serde

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import io.joygraph.core.program.Combinable
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.buffers.streams.CombinerBuffer

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class CombinableAsyncSerializer[I]
(protected[this] val msgType : Byte,
 protected[this] val kryoFactory : () => Kryo,
 protected[this] val bufferExceededThreshold : Int,
 idSerializer : (Kryo, KryoOutput, I) => Any) extends AsyncBufferedSerializer
{

  private[this] val _combinerBuffers : TrieMap[Int, CombinerBuffer[I]] = TrieMap.empty
  private[this] val _kryoOutputs : TrieMap[Int, KryoOutput] = TrieMap.empty
  private[this] val _byteBufferInputs : TrieMap[Int, ByteBufferInput] = TrieMap.empty

  private[this] def byteBufferInputs(index : Int) : ByteBufferInput = {
    _byteBufferInputs.getOrElseUpdate(index, new ByteBufferInput(bufferExceededThreshold))
  }

  private[this] def combinerBuffers(index : Int) : CombinerBuffer[I] = {
    _combinerBuffers.getOrElseUpdate(index, new CombinerBuffer[I](bufferExceededThreshold, idSerializer))
  }

  private[this] def kryoOutputs(index : Int) : KryoOutput = {
    _kryoOutputs.getOrElseUpdate(index, new KryoOutput(bufferExceededThreshold, bufferExceededThreshold))
  }

  def serialize[M](index : ThreadId, workerId : WorkerId, i: I, m : M, messageSerializer : (Kryo, KryoOutput, M) => Any, messageDeserializer : (Kryo, ByteBufferInput) => M)(outputHandler : ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext, combinable : Combinable[M]) : Unit = {
    implicit val kryo = kryos(index)
    val combinerBuffer = combinerBuffers(workerId)
    combinerBuffer.synchronized{
      implicit val kryoOutput = kryoOutputs(workerId)
      implicit val byteBufferInput = byteBufferInputs(workerId)
      if (!combinerBuffer.add(i,m, combinable.combine, messageSerializer, messageDeserializer)) {
        val buffer = buffers(workerId)
        // fill the byteBufferInputs
        combinerBuffer.asByteBuffers().foreach {
          bb =>
            bb.flip()
            buffer.write(bb)
            buffer.increment()
        }
        // empty combinerBuffer
        combinerBuffer.clear()

        flushBuffer(workerId, buffer, outputHandler)

        assert(combinerBuffer.add(i,m, combinable.combine, messageSerializer, messageDeserializer))
      }
    }
  }

  override def sendNonEmptyByteBuffers(outputHandler : ((ByteBuffer, Int)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext) : Unit =
    _combinerBuffers.filter(_._2.size() > 0).foreach {
    case (index, combinerBuffer) =>
      val buffer = buffers(index)
      combinerBuffer.asByteBuffers().foreach {
        bb =>
          bb.flip()
          buffer.write(bb)
          buffer.increment()
      }
      combinerBuffer.clear()

      flushBufferWithWorkerId(index, buffer, outputHandler)
  }
}