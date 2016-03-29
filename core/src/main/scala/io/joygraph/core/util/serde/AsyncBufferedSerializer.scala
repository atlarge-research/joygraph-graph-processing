package io.joygraph.core.util.serde

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferOutputStream

import scala.collection.concurrent.TrieMap

trait AsyncBufferedSerializer {
  private[this] val _bufferSwaps: TrieMap[Int, BlockingQueue[ObjectByteBufferOutputStream]] = TrieMap.empty
  private[this] val _buffers: TrieMap[Int, ObjectByteBufferOutputStream] = TrieMap.empty
  private[this] val _kryos: TrieMap[Int, Kryo] = TrieMap.empty
  val timeSpent = new AtomicLong(0)
  val timeSpentTaking = new AtomicLong(0)

  protected[this] val kryoFactory : () => Kryo
  protected[this] val msgType : Byte
  protected[this] val bufferExceededThreshold : Int

  protected[this] def kryos(index: Int) = {
    _kryos.getOrElseUpdate(index, kryoFactory())
  }

  protected[this] def bufferSwaps(index: Int): BlockingQueue[ObjectByteBufferOutputStream] = {
    _bufferSwaps.getOrElseUpdate(index, {
      val linkedBlockingQueue = new LinkedBlockingQueue[ObjectByteBufferOutputStream]
      // initialize bufferSwap
      linkedBlockingQueue.add(new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold))
      linkedBlockingQueue
    })
  }

  protected[this] def buffers(index: Int): ObjectByteBufferOutputStream = {
    _buffers.getOrElseUpdate(index, new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold))
  }

  protected[this] def returnBuffers(index: Int, os: ObjectByteBufferOutputStream) = {
    _buffers(index) = os
  }

  protected[this] def rawBuffers() : TrieMap[Int, ObjectByteBufferOutputStream] = {
    _buffers
  }
}
