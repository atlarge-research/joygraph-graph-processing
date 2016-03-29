package io.joygraph.core.util.serde

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.{Kryo, KryoException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class AsyncSerializer
(protected[this] val msgType : Byte,
 protected[this] val kryoFactory : () => Kryo,
 protected[this] val bufferExceededThreshold : Int = 1 * 1024 * 1024)
  extends AsyncBufferedSerializer {

  def serialize[T](index: Int, o: T, serializer: (Kryo, Output, T) => Unit)(outputHandler: ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit = {
    val kryo = kryos(index)
    kryo.synchronized {
      val start = System.currentTimeMillis()
      val os = buffers(index)
      // get original position for catch condition
      val originalPos = os.position()
      Try {
        serializer(kryo, os, o)
        os.increment()
      } match {
        case Failure(_: KryoException) => // overflow
          // hand off and swap buffer
          os.setPosition(originalPos)
          os.writeCounter()

          // by using the linkedblockingqueue we ensure that there is at most ONE ByteBuffer being processed
          val bufferSwap = bufferSwaps(index)
          val startTake = System.currentTimeMillis()
          val osSwap = bufferSwap.take()
          val diffTake = System.currentTimeMillis() - startTake
          timeSpentTaking.addAndGet(diffTake)
          returnBuffers(index, osSwap)
          serializer(kryo, osSwap, o) // TODO exception may occur here IFF object does not fit in the output...
          osSwap.increment()
          // hand it off to the output handler, which will forward it to the network stack
          // when the network stack is finished with the bytebuffer, we put it back into the bufferSwap
          outputHandler(os.handOff()).foreach { _ =>
            // reset the osSwap, it's been used and needs to be set to pos 0
            os.resetOOS()
            // buffers in bufferswap are now clean!
            bufferSwap.add(os)
          } // sweet I got it back!)
        case _ =>
      }

      val diff = System.currentTimeMillis() - start
      timeSpent.addAndGet(diff)
    }
  }

  def sendNonEmptyByteBuffers(outputHandler: ((ByteBuffer, Int)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit ={
    rawBuffers().filter(_._2.hasElements).foreach {
      case (index, os) =>
        os.writeCounter()
        outputHandler((os.handOff(), index)).foreach(_ => buffers(index).resetOOS())
    }
  }
}