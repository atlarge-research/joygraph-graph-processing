package io.joygraph.core.util.serde

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.{Kryo, KryoException}
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferOutputStream

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class AsyncSerializer
(protected[this] val msgType : Byte,
 protected[this] val kryoFactory : () => Kryo,
 protected[this] val bufferExceededThreshold : Int = 1 * 1024 * 1024)
  extends AsyncBufferedSerializer {

  def serialize[T](index : ThreadId, workerId: WorkerId, o: T, serializer: (Kryo, Output, T) => Unit)(outputHandler: ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit = {
    val kryo = kryos(index) // zero contention
    kryo.synchronized {
      val start = System.currentTimeMillis()
      val os = buffers(workerId)
      // get original position for catch condition
      val originalPos = os.position()
      Try {
        serializer(kryo, os, o)
        os.increment()
        returnBuffers(workerId, os)
      } match {
        case Failure(_: KryoException) => // overflow
          // hand off
          os.setPosition(originalPos)
          os.writeCounter()

          // hand it off to the output handler, which will forward it to the network stack
          // when the network stack is finished with the bytebuffer, we put it back into the bufferSwap
          outputHandler(os.handOff()).foreach { _ =>
            // reset the osSwap, it's been used and needs to be set to pos 0
            os.resetOOS()
            // buffers in bufferswap are now clean!
            returnBuffers(workerId, os)
          } // sweet I got it back!)

          // TODO if it fails here it means that the message is larger than the entire buffer...
          try {
            val otherOs = buffers(workerId)
            serializer(kryo, otherOs, o)
            otherOs.increment()
            returnBuffers(workerId, otherOs)
          } catch {
            case t : Throwable => t.printStackTrace()
          }
        case Failure(t : Throwable) =>
          println("some other failure what")
          t.printStackTrace()
        case _ =>
          // noop
      }

      val diff = System.currentTimeMillis() - start
      timeSpent.addAndGet(diff)
    }
  }

  def sendNonEmptyByteBuffers(outputHandler: ((ByteBuffer, WorkerId)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit ={
    val emptyBuffers = new Array[ObjectByteBufferOutputStream](numBuffersPerWorkerId)
    rawBuffers().foreach {
      case (workerId, osList) =>
        var continue = true
        var emptyBufferIndex = 0
        while (continue) {
          Option(osList.poll()) match {
            case Some(os) =>
              if (os.hasElements) {
                os.writeCounter()
                outputHandler((os.handOff(), workerId)).foreach{ _ =>
                  os.resetOOS()
                  returnBuffers(workerId, os)
                }
              } else {
                emptyBuffers(emptyBufferIndex) = os
                emptyBufferIndex += 1
              }
            case None =>
              continue = false
          }
        }

        var i = 0
        while(i < emptyBufferIndex) {
          returnBuffers(workerId, emptyBuffers(i))
          i += 1
        }

    }
  }
}