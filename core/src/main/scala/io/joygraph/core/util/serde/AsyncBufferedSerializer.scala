package io.joygraph.core.util.serde

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferOutputStream

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

trait AsyncBufferedSerializer {
  type ThreadId = Int
  type WorkerId = Int

  protected[this] val numBuffersPerWorkerId = 10
  protected[this] val lastResortBufferPerWorkerId = 2
  protected[this] val _initialized : java.util.concurrent.ConcurrentHashMap[WorkerId, Boolean] = new ConcurrentHashMap[WorkerId, Boolean]()
  protected[this] val _buffers: java.util.concurrent.ConcurrentHashMap[WorkerId, LinkedBlockingQueue[ObjectByteBufferOutputStream]] = new ConcurrentHashMap[WorkerId, LinkedBlockingQueue[ObjectByteBufferOutputStream]]()
  protected[this] val _lastResortBuffer: java.util.concurrent.ConcurrentHashMap[WorkerId, LinkedBlockingQueue[ObjectByteBufferOutputStream]] = new ConcurrentHashMap[WorkerId, LinkedBlockingQueue[ObjectByteBufferOutputStream]]()
  private[this] val _kryos: TrieMap[ThreadId, Kryo] = TrieMap.empty
  private[this] val bufferCreationLock = new Object

  val timeSpent = new AtomicLong(0)

  protected[this] val kryoFactory : () => Kryo
  protected[this] val msgType : Byte
  protected[this] val bufferExceededThreshold : Int

  protected[this] def kryos(index: ThreadId) = {
    _kryos.getOrElseUpdate(index, kryoFactory())
  }

  private[this] def createQueue(numBuffers : Int): LinkedBlockingQueue[ObjectByteBufferOutputStream] = {
    val q = new LinkedBlockingQueue[ObjectByteBufferOutputStream](numBuffers)
    var i = 0
    while (i < numBuffers) {
      q.add(new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold))
      i += 1
    }
    q
  }

  /**
    * When an empty buffer is created, it means there are also no queue existing for the buffer.
    * We must construct the queue before the empty buffer.
    */
  private[this] def initializeBuffers(index : WorkerId) :  LinkedBlockingQueue[ObjectByteBufferOutputStream] = {
    bufferCreationLock.synchronized {
      // create empty queue
      _buffers.getOrElseUpdate(index, createQueue(numBuffersPerWorkerId))
      // create queue of empty buffers
      _lastResortBuffer.getOrElseUpdate(index, createQueue(lastResortBufferPerWorkerId))
    }
  }

  /**
    * Buffers originate from "empty" buffers, initially there are only empty buffers
    * thus when there are no buffers, we take entries from empty buffer
    *
    * @param index
    * @return
    */
  protected[this] def buffers(index: WorkerId): ObjectByteBufferOutputStream = {
    Option(_buffers.get(index)) match {
      case Some(queue) =>
        // buffers are already initialized, wait for a buffer
        queue.take()
      case None =>
        // check whether it is initialized
        Option(_initialized.get(index)) match {
          case Some(_) =>
            // initialized
            _buffers(index).take()
          case None =>
            // not initialized
            _initialized.synchronized {
              Option(_initialized.get(index)) match {
                case Some(_) =>
                  // some other thread initialized it
                  _buffers(index).take()
                case None =>
                  // need to initialize
                  initializeBuffers(index)
                  // set to initialized
                  _initialized.put(index, true)
                  _buffers(index).take()
              }
            }
        }
    }
  }

  protected[this] def returnBuffers(index: WorkerId, os: ObjectByteBufferOutputStream) = {
    _buffers(index).add(os)
  }

  protected[this] def waitForEmptyBuffer(index : WorkerId) : ObjectByteBufferOutputStream = {
    _lastResortBuffer(index).take()
  }

  protected[this] def returnLastResortBuffer(index : WorkerId, os: ObjectByteBufferOutputStream) = {
    _lastResortBuffer(index).add(os)
  }

  protected[this] def flushLastResortBuffer
  (workerId : WorkerId,
   os : ObjectByteBufferOutputStream,
   outputHandler: ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit  = {
    os.writeCounter()

    addMessagesSent(workerId, os.counter())
    outputHandler(os.handOff()).foreach { _ =>
      // reset the osSwap, it's been used and needs to be set to pos 0
      os.resetOOS()
      returnLastResortBuffer(workerId, os)
    }
  }

  protected[this] def flushBuffer
  (workerId : WorkerId,
   os : ObjectByteBufferOutputStream,
   outputHandler: ByteBuffer => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit  = {
    os.writeCounter()

    addMessagesSent(workerId, os.counter())
    outputHandler(os.handOff()).foreach { _ =>
      // reset the osSwap, it's been used and needs to be set to pos 0
      os.resetOOS()
      returnBuffers(workerId, os)
    }
  }

  protected[this] def flushBufferWithWorkerId
  (workerId : WorkerId,
   os : ObjectByteBufferOutputStream,
   outputHandler: ((ByteBuffer, WorkerId)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit = {
    os.writeCounter()

    addMessagesSent(workerId, os.counter())
    outputHandler(os.handOff(), workerId).foreach { _ =>
      // reset the osSwap, it's been used and needs to be set to pos 0
      os.resetOOS()
      returnBuffers(workerId, os)
    }
  }

  /**
    * The assumption is that when flushing the buffer, all writing must be seized.
    * Otherwise we cannot guarantee that all buffers are flushed
    */
  def flushNonEmptyByteBuffer
  (workerId : WorkerId)(outputHandler : ((ByteBuffer, WorkerId)) => Future[ByteBuffer])(implicit executionContext: ExecutionContext): Unit = {
    val emptyBuffers = new Array[ObjectByteBufferOutputStream](numBuffersPerWorkerId)
    var emptyBufferIndex = 0

    val osList : LinkedBlockingQueue[ObjectByteBufferOutputStream] = _buffers(workerId)

    while (emptyBufferIndex < numBuffersPerWorkerId) {
      val os = osList.take()
      if (os.hasElements) {
        flushBufferWithWorkerId(workerId, os, outputHandler)
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
    _buffers.keySet().foreach(flushNonEmptyByteBuffer(_)(outputHandler))
  }

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
}
