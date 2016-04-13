package io.joygraph.core.actor.vertices.impl.serialized

import java.lang.Thread.UncaughtExceptionHandler
import java.nio.ByteBuffer
import java.util.concurrent._

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import io.joygraph.core.actor.VertexComputation
import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.actor.vertices.impl.serialized.OpenHashMapSerializedVerticesStore.{Partition, Worker}
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program.{Edge, Vertex, VertexImpl}
import io.joygraph.core.util._
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.serde.AsyncSerializer

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParIterable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object OpenHashMapSerializedVerticesStore {

  protected[OpenHashMapSerializedVerticesStore] class ForeverThread extends Thread {


    // TODO put in a sane number for capacity
    private[this] val taskQueue = new LinkedBlockingQueue[Runnable]()
    @volatile private[this] var running = true
    @volatile private[this] var taskIsRunning = false
    private[this] val waitObject = new Object

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        running = false
        interruptMe()
      }
    })
    private[this] def interruptMe(): Unit = {
      interrupt()
    }

    override def run(): Unit = {
      try {
      var runnable : Runnable = null
      while (running) {
        runnable = taskQueue.take()
        taskIsRunning = true
        runnable.run()
        taskIsRunning = false
        if (taskQueue.isEmpty) {
          waitObject.synchronized{
            waitObject.notify()
          }
        }
      }
      } catch {
        case e : InterruptedException =>
          // noop
      }
    }

    def execute(r : Runnable) : Unit = {
      // pass runnable to thread
      taskQueue.add(r)
    }
  }

  protected[OpenHashMapSerializedVerticesStore] class Worker(name : String, errorReporter : (Throwable) => Unit) {
    private[this] val EXCEPTIONHANDLER = new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = errorReporter(e)
    }

    private[this] val foreverThread = new ForeverThread()
    foreverThread.start()

    def execute(runnable: Runnable): Unit = {
      foreverThread.execute(runnable)
    }

  }

  protected[OpenHashMapSerializedVerticesStore] class Partition[I,V,E]
  (override protected[this] val clazzI: Class[I],
   override protected[this] val clazzE: Class[E],
   override protected[this] val clazzV: Class[V],
   val partitionIndex : Int,
   val totalPartitions : Int) extends VerticesStore[I,V,E] with KryoSerialization  {

    private[this] val _halted : scala.collection.mutable.Map[I, Boolean] = mutable.OpenHashMap.empty
    private[this] val _edges : scala.collection.mutable.Map[I, DirectByteBufferGrowingOutputStream] = mutable.OpenHashMap.empty
    private[this] val _values :  scala.collection.mutable.Map[I, V] = mutable.OpenHashMap.empty
    private[this] val NO_EDGES = Iterable.empty[Edge[I,E]]
    // TODO inject kryo
    private[this] val kryo = new Kryo()
    private[this] val kryoOutput = new KryoOutput(maxMessageSize, maxMessageSize)
    private[this] var numEdges = 0

    private[this] val reusableIterable = new ReusableIterable[Edge[I,E]] {
      private[this] val reusableEdge : Edge[I,E] = Edge[I,E](null.asInstanceOf[I], null.asInstanceOf[E])
      override protected[this] def deserializeObject(): Edge[I, E] = {
        reusableEdge.dst = _kryo.readObject(_input, clazzI)
        if (!isNullClass) {
          reusableEdge.e = _kryo.readObject(_input, clazzE)
        }
        reusableEdge
      }
    }.input(new ByteBufferInput(maxMessageSize))
      .kryo(kryo)


    private[this] val mutableIterable = {
      val mIterable = new MutableReusableIterable[I, Edge[I, E]] {
        override protected[this] def deserializeObject(): Edge[I, E] = {
          if (!isNullClass) {
            Edge(_kryo.readObject(_input, clazzI), _kryo.readObject(_input, clazzE))
          } else {
            Edge(_kryo.readObject(_input, clazzI), null.asInstanceOf[E])
          }
        }

        override def readOnly: Iterable[Edge[I, E]] = throw new UnsupportedOperationException
      }
      mIterable
        .input(new ByteBufferInput(maxMessageSize))
        .kryo(kryo)
      mIterable
    }

    override def halted(vId: I): Boolean = _halted.getOrElse(vId, false)

    override def vertices: Iterable[I] = _edges.keys

    override def removeAllFromVertex(vId: I): Unit = {
      _halted.remove(vId)
      _edges.remove(vId)
      _values.remove(vId)
    }

    override def vertexValue(vId: I): V = _values.getOrElse(vId, null.asInstanceOf[V])

    override def setHalted(vId: I, halted: Boolean): Unit =
      if (halted) {
        _halted(vId) = true
      } else {
        _halted.remove(vId)
      }

    private[this] def edgeStream(vId : I) = _edges.getOrElseUpdate(vId, new DirectByteBufferGrowingOutputStream(0))

    override def edges(vId: I): Iterable[Edge[I, E]] = _edges.get(vId) match {
      case Some(os) =>
        if (os.isEmpty) {
          NO_EDGES
        } else {
          reusableIterable
            .bufferProvider(() => os.getBuf)
        }
      case None => NO_EDGES
    }

    override def releaseEdgesIterable(edgesIterable: Iterable[Edge[I, E]]): Unit = {
      if (edgesIterable eq mutableIterable) {
        val src = mutableIterable.vId
        val os = edgeStream(src)
        if (mutableIterable.hasBeenUsed) {
          os.clear()
          // propagate changes to edges
          mutableIterable.mutatedBuffer.foreach {
            case Edge(dst, value) => serializeEdge(dst, value, os)
          }
          os.trim()
        }

        // release resources
        mutableIterable.reset()
      }
    }

    override def addEdge(src: I, dst: I, value: E): Unit = {
      val os = edgeStream(src)
      serializeEdge(dst, value, os)
      numEdges += 1
    }

    private[this] def serializeEdge(dst : I, value : E, os : DirectByteBufferGrowingOutputStream): Unit = {
      kryoOutput.setOutputStream(os)
      kryo.writeObject(kryoOutput, dst)
      if (!isNullClass) {
        kryo.writeObject(kryoOutput, value)
      }
      kryoOutput.flush()
      os.trim()
    }

    override def localNumEdges: WorkerId = numEdges

    override def localNumVertices: WorkerId = _edges.keys.size

    override def mutableEdges(vId: I): Iterable[Edge[I, E]] = _edges.get(vId) match {
      case Some(os) =>
        mutableIterable
          .vId(vId)
          .bufferProvider(() => os.getBuf)
      case None =>
        NO_EDGES
    }

    override def addVertex(vertex: I): Unit = edgeStream(vertex)

    override def setVertexValue(vId: I, v: V): Unit = _values(vId) = v

    override def parVertices: ParIterable[I] = throw new UnsupportedOperationException

    override def computeVertices(computation: VertexComputation[I, V, E]): Boolean = {
      val verticesStore = this
      val simpleVertexInstancePool = new SimplePool[Vertex[I,V,E]](new VertexImpl[I,V,E] {
        override def addEdge(dst: I, e: E): Unit = {
          verticesStore.addEdge(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
        }
      })
      vertices.foreach(computation.computeVertex(_, verticesStore, simpleVertexInstancePool))
      computation.hasHalted
    }

    override def distributeVertices
    (newWorkersMap: Map[WorkerId, Boolean],
     haltedAsyncSerializer: AsyncSerializer,
     idAsyncSerializer: AsyncSerializer,
     valueAsyncSerializer: AsyncSerializer,
     edgesAsyncSerializer: AsyncSerializer,
     partitioner: VertexPartitioner,
     outputHandler: (ByteBuffer, WorkerId) => Future[ByteBuffer],
     messageStore: MessageStore,
     messagesAsyncSerializer: AsyncSerializer,
     currentOutgoingMessageClass: Class[_])(implicit exeContext : ExecutionContext): Unit = {

      val verticesToBeRemoved = TrieMap.empty[ThreadId, ArrayBuffer[I]]
      val threadId = ThreadId.getMod(totalPartitions)
      vertices.foreach{ vId =>
        val workerId = partitioner.destination(vId)
        if (newWorkersMap(workerId)) {
          verticesToBeRemoved.getOrElseUpdate(threadId, ArrayBuffer.empty[I]) += vId

          distributeVertex(
            vId,
            workerId,
            threadId,
            haltedAsyncSerializer,
            idAsyncSerializer,
            valueAsyncSerializer,
            edgesAsyncSerializer,
            outputHandler,
            messageStore,
            messagesAsyncSerializer,
            currentOutgoingMessageClass
          )
        }
      }
      verticesToBeRemoved.par.foreach(_._2.foreach(removeAllFromVertex))
    }
  }
}

class OpenHashMapSerializedVerticesStore[I,V,E]
(protected[this] val clazzI : Class[I],
 protected[this] val clazzE : Class[E],
 protected[this] val clazzV : Class[V],
 numPartitions : Int,
 errorReporter : (Throwable) => Unit
) extends VerticesStore[I,V,E] {

  private[this] val workers = new Array[Worker](numPartitions)
  private[this] val partitions = new Array[Partition[I,V,E]](numPartitions)
  for(i <- 0 until numPartitions) {
    workers(i) = new Worker("vertex-partition-" + i, errorReporter)
    partitions(i) = new Partition[I, V, E](clazzI, clazzE, clazzV, i, numPartitions)
  }

  private[this] def partition(vId : I) : Partition[I,V,E] = {
    val partId = vId.hashCode() % numPartitions
    partitions(partId)
  }

  override def removeAllFromVertex(vId: I): Unit = {
    val part = partition(vId)
    val worker = workers(part.partitionIndex)
    val promise = Promise[Unit]
    worker.execute(new Runnable {
      override def run(): Unit = {
        part.removeAllFromVertex(vId)
        promise.success()
      }
    })
    Await.ready(promise.future, Duration.Inf)
  }

  override def halted(vId: I): Boolean = throw new UnsupportedOperationException

  override def vertices: Iterable[I] = {
    new Iterable[I] {
      override def iterator: Iterator[I] = partitions.map(_.vertices.toIterator).reduce(_ ++ _)
    }
  }

  override def vertexValue(vId: I): V = partition(vId).vertexValue(vId)

  override def setHalted(vId: I, halted: Boolean): Unit = {
    val part = partition(vId)
    val worker = workers(part.partitionIndex)
    val promise = Promise[Unit]
    worker.execute(new Runnable {
      override def run(): Unit = {
        part.setHalted(vId, halted)
        promise.success()
      }
    })
    Await.ready(promise.future, Duration.Inf)
  }

  override def edges(vId: I): Iterable[Edge[I, E]] = partition(vId).edges(vId)

  override def releaseEdgesIterable(edgesIterable: Iterable[Edge[I, E]]): Unit = throw new UnsupportedOperationException

  override def addEdge(src: I, dst: I, value: E): Unit = {
    val part = partition(src)
    val worker = workers(part.partitionIndex)
    val promise = Promise[Unit]
    worker.execute(new Runnable {
      override def run(): Unit = {
        part.addEdge(src, dst, value)
        promise.success()
      }
    })
    Await.ready(promise.future, Duration.Inf)
  }

  override def localNumEdges: WorkerId = partitions.map(_.localNumEdges).sum

  override def localNumVertices: WorkerId = partitions.map(_.localNumVertices).sum

  override def addVertex(vertex: I): Unit = {
    val part = partition(vertex)
    val worker = workers(part.partitionIndex)
    val promise = Promise[Unit]
    worker.execute(new Runnable {
      override def run(): Unit = {
        part.addVertex(vertex)
        promise.success()
      }
    })
    Await.ready(promise.future, Duration.Inf)
  }

  override def mutableEdges(vId: I): Iterable[Edge[I, E]] = throw new UnsupportedOperationException

  override def setVertexValue(vId: I, v: V): Unit = {
    val part = partition(vId)
    val worker = workers(part.partitionIndex)
    val promise = Promise[Unit]
    worker.execute(new Runnable {
      override def run(): Unit = {
        part.setVertexValue(vId, v)
        promise.success()
      }
    })
    Await.ready(promise.future, Duration.Inf)
  }

  override def parVertices: ParIterable[I] = throw new UnsupportedOperationException

  override def computeVertices(computation: VertexComputation[I, V, E]): Boolean = {
     workers.zipWithIndex.map {
      case (worker, index) =>
        val promise = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            partitions(index).computeVertices(computation)
            promise.success()
          }
        })
        promise.future
    }.foreach(Await.ready(_, Duration.Inf))
    // TODO abort on exception
    computation.hasHalted
  }

  override def distributeVertices
  (newWorkersMap: Map[WorkerId, Boolean],
   haltedAsyncSerializer: AsyncSerializer,
   idAsyncSerializer: AsyncSerializer,
   valueAsyncSerializer: AsyncSerializer,
   edgesAsyncSerializer: AsyncSerializer,
   hashPartitioner: VertexPartitioner,
   outputHandler: (ByteBuffer, WorkerId) => Future[ByteBuffer],
   messageStore: MessageStore,
   messagesAsyncSerializer: AsyncSerializer,
   currentOutgoingMessageClass: Class[_])(implicit exeContext : ExecutionContext): Unit = {
    workers.zipWithIndex.map {
      case (worker, index) =>
        val promise = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            partitions(index).distributeVertices(
              newWorkersMap,
              haltedAsyncSerializer,
              idAsyncSerializer,
              valueAsyncSerializer,
              edgesAsyncSerializer,
              hashPartitioner,
              outputHandler,
              messageStore,
              messagesAsyncSerializer,
              currentOutgoingMessageClass
            )
            promise.success()
          }
        })
        promise.future
    }.foreach(Await.ready(_, Duration.Inf))
  }
}

