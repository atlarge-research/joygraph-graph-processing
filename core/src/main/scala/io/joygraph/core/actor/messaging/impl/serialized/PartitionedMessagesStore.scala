package io.joygraph.core.actor.messaging.impl.serialized

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.actor.messaging.{Message, MessageStore}
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.util.DirectByteBufferGrowingOutputStream
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.concurrency.PartitionWorker

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

object PartitionedMessagesStore {
  abstract class Partition
  (val partitionIndex : Int,
   val totalPartitions : Int,
   maxMessageSize : Int) extends MessageStore {
    def transferNextMessagesTo(partition: Partition): Unit = {
      nextMessages.foreach{
        case (key, stream) =>
          partition.nextMessagesRaw(key) = stream
      }
      nextMessages.clear()
    }

    private[this] var nextMessages : mutable.Map[Any, DirectByteBufferGrowingOutputStream] = createNextMessages
    private[this] var currentMessages : mutable.Map[Any, DirectByteBufferGrowingOutputStream] = createCurrentMessages
    private[this] val EMPTY_MESSAGES : Iterable[Any] = Iterable.empty[Any]
    private[this] var _reusableIterableThreadLocal : ThreadLocal[ReusableIterable[_]] = _
    // TODO inject factory from worker
    // Reading and writing can occur at the same time, so we need 2 instances of Kryo.
    private[this] val kryoReadFactory = () => new Kryo()
    private[this] val kryoWriteThreadLocal = new ThreadLocal[Kryo] {
      override def initialValue(): Kryo = new Kryo()
    }
    // buffersize should be the same as the AsyncSerializer
    private[this] val kryoOutputThreadLocal = new ThreadLocal[KryoOutput] {
      override def initialValue(): KryoOutput = new KryoOutput(maxMessageSize, maxMessageSize)
    }

    def createNextMessages : mutable.Map[Any, DirectByteBufferGrowingOutputStream]
    def createCurrentMessages : mutable.Map[Any, DirectByteBufferGrowingOutputStream]

    override def setReusableIterableFactory(factory: => ReusableIterable[Any]): Unit = {
      _reusableIterableThreadLocal = new ThreadLocal[ReusableIterable[_]] {
        override def initialValue(): ReusableIterable[_] = {
          factory.kryo(kryoReadFactory())
        }
      }
    }
    private[this] def reusableIterable[M](clazz : Class[M]) : ReusableIterable[M] = {
      _reusableIterableThreadLocal.get().asInstanceOf[ReusableIterable[M]]
    }

    protected[this] def postHandleMessage[I](dst : I, os : DirectByteBufferGrowingOutputStream) : Unit = {
      // noop
    }

    override def _handleMessage[I](index: WorkerId, dstMPair: Message[I], clazzI: Class[I], clazzM: Class[_]): Unit = {
      val os = nextMessages.getOrElseUpdate(dstMPair.dst, new DirectByteBufferGrowingOutputStream(8))
      val kryoOutput = kryoOutputThreadLocal.get
      kryoOutput.setOutputStream(os)
      kryoWriteThreadLocal.get().writeObject(kryoOutput, dstMPair.msg)
      kryoOutput.flush()
      // TODO evaluate the removal of trim
      //    outputStream.trim()
      postHandleMessage(dstMPair.dst, os)
    }

    override def releaseMessages(messages: Iterable[_], clazz: Class[_]): Unit = {
      // noop
    }

    override protected[messaging] def removeMessages[I](dst: I): Unit = synchronized {
      currentMessages.remove(dst) match {
        case Some(x) =>
          x.destroy()
        case None =>
      }
    }

    override protected[messaging] def removeNextMessages[I](dst: I): Unit = synchronized {
      nextMessages.remove(dst) match {
        case Some(x) =>
          x.destroy()
        case None =>
      }
    }

    def nextMessagesRaw = nextMessages

    override def nextMessages[I, M](dst: I, clazzM: Class[M]): Iterable[M] = {
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

    override def removeNextMessages(workerId: WorkerId, partitioner: VertexPartitioner): Unit = {
      nextMessages
        .keys
        .filter(vId => partitioner.destination(vId) != workerId)
        .foreach(vId => removeNextMessages(vId))
    }
  }
}

abstract class PartitionedMessagesStore
(numPartitions : Int,
 maxMessageSize : Int,
 errorReporter : (Throwable) => Unit) extends MessageStore {

  private[this] val workers = new Array[PartitionWorker](numPartitions)
  private[this] val _partitions = new Array[PartitionedMessagesStore.Partition](numPartitions)
  for(i <- 0 until numPartitions) {
    workers(i) = new PartitionWorker("message-partition-" + i, errorReporter)
    _partitions(i) = createPartitionInstance(i, numPartitions, maxMessageSize)
  }

  def createPartitionInstance(index : Int, numPartitions : Int, maxEdgeSize : Int) : PartitionedMessagesStore.Partition

  def partitions : Array[PartitionedMessagesStore.Partition] = {
    _partitions
  }

  private[this] def partition(vId : Any) : PartitionedMessagesStore.Partition = {
    val partId = vId.hashCode() % numPartitions
    _partitions(partId)
  }

  /**
    * Pooling for serialized message iterables
    */
  override def setReusableIterableFactory(factory: => ReusableIterable[Any]): Unit = {
    _partitions.foreach(_.setReusableIterableFactory(factory))
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
    partition(dst).removeMessages(dst)
  }

  override def nextMessages[I, M](dst: I, clazzM: Class[M]): Iterable[M] = {
    partition(dst).nextMessages(dst, clazzM)
  }

  override def emptyNextMessages: Boolean = _partitions.map(_.emptyNextMessages).reduce(_ && _)

  override def messagesOnBarrier(): Unit = {
    workers.zipWithIndex.map {
      case (worker, index) =>
        val p = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            _partitions(index).messagesOnBarrier()
            p.success()
          }

        })
        p.future
    }.foreach(Await.ready(_, Duration.Inf))
  }

  override def messages[I, M](dst: I, clazzM: Class[M]): Iterable[M] = partition(dst).messages(dst, clazzM)

  override protected[messaging] def removeNextMessages[I](dst: I): Unit = ???

  override def removeNextMessages(workerId: WorkerId, partitioner: VertexPartitioner): Unit = {
    workers.zipWithIndex.map {
      case (worker, index) =>
        val p = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            _partitions(index).removeNextMessages(workerId, partitioner)
            p.success()
          }

        })
        p.future
    }.foreach(Await.ready(_, Duration.Inf))
  }

  override def addAllNextMessages(other: MessageStore): Unit = {
    other match {
      case otherStore : PartitionedMessagesStore =>
        otherStore.partitions.zipWithIndex.foreach{
          case (otherPart, index) =>
            otherPart.transferNextMessagesTo(partitions(index))
        }
      case _ =>
    }
  }
}
