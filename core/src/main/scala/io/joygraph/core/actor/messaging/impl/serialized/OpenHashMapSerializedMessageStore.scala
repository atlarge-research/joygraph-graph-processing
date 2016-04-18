package io.joygraph.core.actor.messaging.impl.serialized

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.actor.messaging.impl.serialized.OpenHashMapSerializedMessageStore.Partition
import io.joygraph.core.actor.messaging.{Message, MessageStore}
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.concurrency.PartitionWorker
import io.joygraph.core.util.{DirectByteBufferGrowingOutputStream, KryoSerialization}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

object OpenHashMapSerializedMessageStore {
  private[OpenHashMapSerializedMessageStore] class Partition
  (val partitionIndex : Int,
   val totalPartitions : Int) extends MessageStore with KryoSerialization {
    private[this] var nextMessages = mutable.OpenHashMap.empty[Any, DirectByteBufferGrowingOutputStream]
    private[this] var currentMessages = mutable.OpenHashMap.empty[Any, DirectByteBufferGrowingOutputStream]
    private[this] val EMPTY_MESSAGES : Iterable[Any] = Iterable.empty[Any]
    private[this] var _reusableIterable : ReusableIterable[_] = _
    // TODO inject factory from worker
    // Reading and writing can occur at the same time, so we need 2 instances of Kryo.
    private[this] val kryoRead = new Kryo()
    private[this] val kryoWrite = new Kryo()
    // buffersize should be the same as the AsyncSerializer
    private[this] val kryoOutput = new KryoOutput(1 * 1024 * 1024, 1 * 1024 * 1024)

    override def setReusableIterableFactory(factory: => ReusableIterable[Any]): Unit = {
      _reusableIterable = factory
      _reusableIterable.kryo(kryoRead)
    }
    private[this] def reusableIterable[M](clazz : Class[M]) : ReusableIterable[M] = {
      _reusableIterable.asInstanceOf[ReusableIterable[M]]
    }

    override def _handleMessage[I](index: WorkerId, dstMPair: Message[I], clazzI: Class[I], clazzM: Class[_]): Unit = {
      val os = nextMessages.getOrElseUpdate(dstMPair.dst, new DirectByteBufferGrowingOutputStream(8))
      kryoOutput.setOutputStream(os)
      kryoWrite.writeObject(kryoOutput, dstMPair.msg)
      kryoOutput.flush()
      // TODO evaluate the removal of trim
      //    outputStream.trim()
    }

    override def releaseMessages(messages: Iterable[_], clazz: Class[_]): Unit = {
      // noop
    }

    override protected[messaging] def removeMessages[I](dst: I): Unit = currentMessages.remove(dst)

    override protected[messaging] def nextMessages[I, M](dst: I, clazzM: Class[M]): Iterable[M] = {
      nextMessages.get(dst) match {
        case Some(os) =>
          reusableIterable(clazzM).bufferProvider(() => os.getBuf)
        case None => EMPTY_MESSAGES.asInstanceOf[Iterable[M]]
      }
    }

    override def emptyNextMessages: Boolean = nextMessages.isEmpty

    override def messagesOnBarrier(): Unit = {
      currentMessages.foreach(_._2.destroy())
      currentMessages = nextMessages
      nextMessages = mutable.OpenHashMap.empty[Any, DirectByteBufferGrowingOutputStream]
    }

    override def messages[I, M](dst: I, clazzM: Class[M]): Iterable[M] = {
      currentMessages.get(dst) match {
        case Some(os) =>
          reusableIterable(clazzM).bufferProvider(() => os.getBuf)
        case None => EMPTY_MESSAGES.asInstanceOf[Iterable[M]]
      }
    }
  }
}

class OpenHashMapSerializedMessageStore
(numPartitions : Int,
 errorReporter : (Throwable) => Unit) extends MessageStore {

  private[this] val workers = new Array[PartitionWorker](numPartitions)
  private[this] val partitions = new Array[Partition](numPartitions)
  for(i <- 0 until numPartitions) {
    workers(i) = new PartitionWorker("message-partition-" + i, errorReporter)
    partitions(i) = new Partition(i, numPartitions)
  }

  private[this] def partition(vId : Any) : Partition = {
    val partId = vId.hashCode() % numPartitions
    partitions(partId)
  }

  /**
    * Pooling for serialized message iterables
    */
  override def setReusableIterableFactory(factory: => ReusableIterable[Any]): Unit = {
    partitions.foreach(_.setReusableIterableFactory(factory))
  }

  override def _handleMessage[I](index: WorkerId, dstMPair: Message[I], clazzI: Class[I], clazzM: Class[_]): Unit = {
    val part = partition(dstMPair.dst)
    part.synchronized {
      part._handleMessage(index, dstMPair, clazzI, clazzM)
    }
  }

  override def releaseMessages(messages: Iterable[_], clazz: Class[_]): Unit = {
    // noop
  }

  override protected[messaging] def removeMessages[I](dst: I): Unit = {
    throw new UnsupportedOperationException
  }

  override protected[messaging] def nextMessages[I, M](dst: I, clazzM: Class[M]): Iterable[M] = {
    partition(dst).nextMessages(dst, clazzM)
  }

  override def emptyNextMessages: Boolean = partitions.map(_.emptyNextMessages).reduce(_ && _)

  override def messagesOnBarrier(): Unit = {
    workers.zipWithIndex.map {
      case (worker, index) =>
        val p = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            partitions(index).messagesOnBarrier()
            p.success()
          }

        })
        p.future
    }.foreach(Await.ready(_, Duration.Inf))
  }

  override def messages[I, M](dst: I, clazzM: Class[M]): Iterable[M] = partition(dst).messages(dst, clazzM)

}
