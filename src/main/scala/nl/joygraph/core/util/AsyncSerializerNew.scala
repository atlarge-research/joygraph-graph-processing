package nl.joygraph.core.util

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.{Kryo, KryoException}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class AsyncSerializerNew[T](msgType : Byte, n : Int, kryoFactory : => Kryo, val bufferExceededThreshold : Int = 10 * 1024 * 1024) {

  private[this] val _buffers : ArrayBuffer[ObjectByteBufferOutputStream] = ArrayBuffer.fill(n)(new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold))
  private[this] val bufferSwaps : ArrayBuffer[BlockingQueue[ObjectByteBufferOutputStream]] = ArrayBuffer.fill(n)(new LinkedBlockingQueue[ObjectByteBufferOutputStream])
  private[this] val kryos : ArrayBuffer[Kryo] = ArrayBuffer.fill(n)(kryoFactory)
  private[this] val locks : ArrayBuffer[Object] = ArrayBuffer.fill(n)(new Object)
  val timeSpent = new AtomicLong(0)

  // initialize bufferSwap
  bufferSwaps.foreach(_.add(new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold)))

  def serialize(index : Int, o: T, serializer : (Kryo, Output, T) => Unit)(outputHandler : ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext) : Unit = {
    locks(index).synchronized {
      val start = System.currentTimeMillis()
      val os = _buffers(index)
      val kryo = kryos(index)
      // get original position for catch condition
      val originalPos = os.position()
      try {
        serializer(kryo, os, o)
        os.increment()
        val diff = System.currentTimeMillis() - start
        timeSpent.addAndGet(diff)
      } catch {
        case e : KryoException => // overflow
          println("KryoException " + e)
          // hand off and swap buffer
          os.setPosition(originalPos)
          os.writeCounter()

          // by using the linkedblockingqueue we ensure that there is at most ONE ByteBuffer being processed
          val bufferSwap = bufferSwaps(index)
          val osSwap = bufferSwap.take()
          _buffers(index) = osSwap
          // reset the osSwap, it's been used and needs to be set to pos 0
          osSwap.resetOOS()
          serializer(kryo, osSwap, o) // TODO exception may occur here IFF message does not fit in the output...
          osSwap.increment()
          val diff = System.currentTimeMillis() - start
          timeSpent.addAndGet(diff)
          // hand it off to the output handler, which will forward it to the network stack
          // when the network stack is finished with the bytebuffer, we put it back into the bufferSwap
          outputHandler(os.handOff()).foreach(_ => bufferSwap.add(os))// sweet I got it back!)
      }

    }
  }

  def currentNonEmptyByteBuffers(): Seq[(ByteBuffer, Int)] = _buffers.filter(_.hasElements).zipWithIndex.map{
    case (a, b) =>
      a.writeCounter()
      (a.handOff(), b)
  }
}