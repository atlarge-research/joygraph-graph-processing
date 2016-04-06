package io.joygraph.core.util.serde

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import io.joygraph.core.util.buffers.KryoOutput.KryoOutputOverflowException
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferOutputStream
import io.joygraph.core.util.serde.AsyncSerializer.AsyncSerializerException

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

object AsyncSerializer {
  private class AsyncSerializerException(msg : String) extends Exception(msg) {

  }
}

class AsyncSerializer
(protected[this] val msgType : Byte,
 protected[this] val kryoFactory : () => Kryo,
 protected[this] val bufferExceededThreshold : Int = 1 * 1024 * 1024)
  extends AsyncBufferedSerializer {

  private[this] val maxRetries = 10

  @tailrec
  private[this] def serialize[T](kryo : Kryo, workerId: WorkerId, o: T, serializer: (Kryo, Output, T) => Unit, tryCount : Int = 1)(outputHandler: ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit = {
    if (tryCount >= maxRetries) {
      throw new AsyncSerializerException("Max tries exceeded, could not serialize object" + o)
    }

    val os = buffers(workerId)
    // get original position for catch condition
    val originalPos = os.position()
    Try {
      serializer(kryo, os, o)
      os.increment()
      returnBuffers(workerId, os)
    } match {
      case Failure(_: KryoOutputOverflowException) => // overflow
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

        serialize(kryo, workerId, o, serializer, tryCount + 1)(outputHandler)
      case Failure(t : Throwable) =>
        println("some other failure what")
        t.printStackTrace()
        throw t
      case _ =>
      // noop
    }
  }

  def serialize[T](index : ThreadId, workerId: WorkerId, o: T, serializer: (Kryo, Output, T) => Unit)(outputHandler: ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit = {
    val kryo = kryos(index) // zero contention
    kryo.synchronized {
      val start = System.currentTimeMillis()
      serialize(kryo, workerId, o, serializer)(outputHandler)
      val diff = System.currentTimeMillis() - start
      timeSpent.addAndGet(diff)
    }
  }

  def sendNonEmptyByteBuffers(outputHandler: ((ByteBuffer, WorkerId)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit ={
    val emptyBuffers = new Array[ObjectByteBufferOutputStream](numBuffersPerWorkerId)
    rawBuffers().foreach {
      case (workerId, osList) =>
        var emptyBufferIndex = 0
        while (emptyBufferIndex < numBuffersPerWorkerId) {
          val os = osList.take()
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
        }

        var i = 0
        while(i < emptyBufferIndex) {
          returnBuffers(workerId, emptyBuffers(i))
          i += 1
        }

    }
  }
}