package io.joygraph.core.util.serde

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import io.joygraph.core.util.buffers.KryoOutput.KryoOutputOverflowException
import io.joygraph.core.util.serde.AsyncSerializer.AsyncSerializerException

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.{Failure, Try}

object AsyncSerializer {
  private class AsyncSerializerException(msg : String) extends Exception(msg) {

  }
}

class AsyncSerializer
(protected[this] val msgType : Byte,
 protected[this] val kryoFactory : () => Kryo,
 protected[this] val bufferExceededThreshold : Int)
  extends AsyncBufferedSerializer {

  private[this] val maxRetries = 10

  @tailrec
  private[this] def serialize[T](kryo : Kryo, workerId: WorkerId, o: T, serializer: (Kryo, Output, T) => Unit, tryCount : Int = 1)(outputHandler: ByteBuffer => Future[ByteBuffer]): Unit = {
    // message is too large even for a new buffer <-- abort
    if (tryCount >= maxRetries) {
      throw new AsyncSerializerException("Max tries exceeded, could not serialize object" + o)
    }

    val useLastResort = tryCount == maxRetries - 1
    val os = if (useLastResort) {
      // try to serialize with an empty buffer
      waitForEmptyBuffer(workerId)
    } else {
      buffers(workerId)
    }

    // get original position for catch condition
    val originalPos = os.position()
    Try {
      serializer(kryo, os, o)
      os.increment()

      if(useLastResort) {
        // flush it
        flushLastResortBuffer(workerId, os, outputHandler)
      } else {
        // return non-full buffer
        returnBuffers(workerId, os)
      }
    } match {
      case Failure(_: KryoOutputOverflowException) => // overflow
        // set position to originalPosition (sane data)
        os.setPosition(originalPos)
        flushBuffer(workerId, os, outputHandler)

        // try to serialize object again
        serialize(kryo, workerId, o, serializer, tryCount + 1)(outputHandler)
      case Failure(t : Throwable) =>
        println("some other failure what")
        t.printStackTrace()
        throw t
      case _ =>
      // noop
    }
  }

  def serialize[T](index : ThreadId, workerId: WorkerId, o: T, serializer: (Kryo, Output, T) => Unit)(outputHandler: ByteBuffer => Future[ByteBuffer]): Unit = {
    val kryo = kryos(index) // zero contention
    kryo.synchronized {
      val start = System.currentTimeMillis()
      serialize(kryo, workerId, o, serializer)(outputHandler)
      val diff = System.currentTimeMillis() - start
      timeSpent.addAndGet(diff)
    }
  }

}