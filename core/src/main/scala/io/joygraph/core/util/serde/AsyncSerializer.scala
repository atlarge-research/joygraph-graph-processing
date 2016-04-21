package io.joygraph.core.util.serde

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import io.joygraph.core.util.buffers.KryoOutput.KryoOutputOverflowException
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferOutputStream
import io.joygraph.core.util.serde.AsyncSerializer.AsyncSerializerException

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
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

  private[this] val messagesSent = TrieMap.empty[WorkerId, AtomicLong]

  def resetMessageSentCounters(): Unit = {
    messagesSent.clear()
  }

  private[this] def addMessagesSent(workerId : WorkerId, numMessages : Int): Unit = {
    messagesSent.getOrElseUpdate(workerId, new AtomicLong(0)).addAndGet(numMessages)
  }

  def getMessagesSent(workerId : WorkerId) : Long = messagesSent(workerId).get()
  def getAllMessagesSent() : scala.collection.Map[WorkerId, Long] = {
    // create copy
    val copy = new mutable.OpenHashMap[Int, Long](messagesSent.size)
    messagesSent.foreach { case (key, value) =>
      copy += key -> value.get
    }
    copy
  }

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
        addMessagesSent(workerId, os.counter())
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

  def flushNonEmptyByteBuffer
  (workerId : WorkerId)
  (outputHandler : ((ByteBuffer, WorkerId)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit = {
    flushNonEmptyByteBuffer(workerId, rawBuffers()(workerId), outputHandler)
  }

  private[this] def flushNonEmptyByteBuffer
  (workerId : WorkerId,
   osList : LinkedBlockingQueue[ObjectByteBufferOutputStream],
   outputHandler : ((ByteBuffer, WorkerId)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit = {
    val emptyBuffers = new Array[ObjectByteBufferOutputStream](numBuffersPerWorkerId)
    var emptyBufferIndex = 0
    while (emptyBufferIndex < numBuffersPerWorkerId) {
      val os = osList.take()
      if (os.hasElements) {
        os.writeCounter()
        addMessagesSent(workerId, os.counter())
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

  def sendNonEmptyByteBuffers(outputHandler: ((ByteBuffer, WorkerId)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit ={
    rawBuffers().foreach {
      case (workerId, osList) =>
        flushNonEmptyByteBuffer(workerId, osList, outputHandler)
    }
  }
}