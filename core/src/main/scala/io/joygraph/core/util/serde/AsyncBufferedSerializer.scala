package io.joygraph.core.util.serde

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferOutputStream

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap

trait AsyncBufferedSerializer {
  type ThreadId = Int
  type WorkerId = Int

  protected[this] val numBuffersPerWorkerId = 2
  protected[this] val _buffers: java.util.concurrent.ConcurrentHashMap[WorkerId, LinkedBlockingQueue[ObjectByteBufferOutputStream]] = new ConcurrentHashMap[WorkerId, LinkedBlockingQueue[ObjectByteBufferOutputStream]]()
  private[this] val _kryos: TrieMap[ThreadId, Kryo] = TrieMap.empty
  private[this] val bufferCreationLock = new Object

  val timeSpent = new AtomicLong(0)

  protected[this] val kryoFactory : () => Kryo
  protected[this] val msgType : Byte
  protected[this] val bufferExceededThreshold : Int

  protected[this] def kryos(index: ThreadId) = {
    _kryos.getOrElseUpdate(index, kryoFactory())
  }

  private[this] def getOrElseUpdate(index : WorkerId) : ObjectByteBufferOutputStream =  {
    val buffer = Option(_buffers.get(index)) match {
      case Some(x) =>
        x
      case None =>
        bufferCreationLock.synchronized {
          _buffers.getOrElseUpdate(index, {
              val linkedBlockingQueue = new LinkedBlockingQueue[ObjectByteBufferOutputStream](numBuffersPerWorkerId)
              // initialize buffer with 2 entries
              val os1 = new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold)
              val os2 = new ObjectByteBufferOutputStream(msgType, bufferExceededThreshold)
              linkedBlockingQueue.add(os1)
              linkedBlockingQueue.add(os2)
              linkedBlockingQueue
          })
        }
    }
    buffer.take()
  }

  protected[this] def buffers(index: WorkerId): ObjectByteBufferOutputStream = {
    getOrElseUpdate(index)
  }

  protected[this] def returnBuffers(index: WorkerId, os: ObjectByteBufferOutputStream) = {
    _buffers(index).add(os)
  }

  protected[this] def rawBuffers() : scala.collection.mutable.Map[WorkerId, LinkedBlockingQueue[ObjectByteBufferOutputStream]] = {
    _buffers
  }
}
