package io.joygraph.core.actor.vertices.impl.serialized

import java.lang.Thread.UncaughtExceptionHandler
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.CountDownLatch

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.actor.vertices.impl.serialized.JavaHashMapSerializedVerticesStore.Partition
import io.joygraph.core.actor.{PregelVertexComputation, QueryAnswerVertexComputation}
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import io.joygraph.core.program.{Edge, Vertex, VertexImpl}
import io.joygraph.core.util._
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.concurrency.PartitionWorker
import io.joygraph.core.util.serde.AsyncSerializer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParIterable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object JavaHashMapSerializedVerticesStore {

  protected[JavaHashMapSerializedVerticesStore] class Partition[I,V,E]
  (override protected[this] val clazzI: Class[I],
   override protected[this] val clazzE: Class[E],
   override protected[this] val clazzV: Class[V],
   val partitionIndex : Int,
   val totalPartitions : Int,
   maxEdgeSize : Int
  ) extends VerticesStore[I,V,E] {

    private[this] val _halted : scala.collection.mutable.Map[I, Boolean] = mutable.OpenHashMap.empty
    private[this] val _edges : java.util.HashMap[I, DirectByteBufferGrowingOutputStream] = new util.HashMap[I, DirectByteBufferGrowingOutputStream]()
    private[this] val _values :  scala.collection.mutable.Map[I, V] = mutable.OpenHashMap.empty
    private[this] val NO_EDGES = Iterable.empty[Edge[I,E]]
    // TODO inject kryo
    private[this] val kryoThreadLocal = new ThreadLocal[Kryo] {
      override def initialValue(): Kryo = {
        new Kryo()
      }
    }
    private[this] val kryoOutputThreadLocal = new ThreadLocal[KryoOutput] {
      override def initialValue(): KryoOutput = new KryoOutput(maxEdgeSize, maxEdgeSize)
    }
    private[this] var numEdges = 0
    private[this] val reusableIterableThreadLocal = new ThreadLocal[ReusableIterable[Edge[I,E]]] {
      override def initialValue(): ReusableIterable[Edge[I, E]] = {
        new ReusableIterable[Edge[I,E]] {
          private[this] val reusableEdge : Edge[I,E] = Edge[I,E](null.asInstanceOf[I], null.asInstanceOf[E])
          override protected[this] def deserializeObject(): Edge[I, E] = {
            reusableEdge.dst = _kryo.readObject(_input, clazzI)
            if (!voidOrUnitClass) {
              reusableEdge.e = _kryo.readObject(_input, clazzE)
            }
            reusableEdge
          }
        }.input(new ByteBufferInput(maxEdgeSize))
          .kryo(new Kryo)
      }
    }

    private[this] val mutableReusableIterableThreadLocal =  new ThreadLocal[MutableReusableIterable[I, Edge[I, E]]]{
      override def initialValue(): MutableReusableIterable[I, Edge[I, E]] = {
        val mIterable = new MutableReusableIterable[I, Edge[I, E]] {
          override protected[this] def deserializeObject(): Edge[I, E] = {
            if (!voidOrUnitClass) {
              Edge(_kryo.readObject(_input, clazzI), _kryo.readObject(_input, clazzE))
            } else {
              Edge(_kryo.readObject(_input, clazzI), null.asInstanceOf[E])
            }
          }

          override def readOnly: Iterable[Edge[I, E]] = throw new UnsupportedOperationException
        }
        mIterable
          .input(new ByteBufferInput(maxEdgeSize))
          .kryo(new Kryo)
        mIterable
      }
    }

    override def halted(vId: I): Boolean = _halted.getOrElse(vId, false)

    override def vertices: Iterable[I] = new Iterable[I] {
      override def iterator: Iterator[I] = new Iterator[I] {
        val it = _edges.keySet().iterator()
        override def hasNext: Boolean = it.hasNext

        override def next(): I = it.next()
      }
    }

    def rawEdges = _edges

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

    private[this] def edgeStream(vId : I) = {
      _edges.computeIfAbsent(vId, _ => new DirectByteBufferGrowingOutputStream(0))
    }

    def explicitlyScopedEdges[T](vId: I)(f : Iterable[Edge[I, E]] => T) : T = {
      val edges = Option(_edges.get(vId)) match  {
        case Some(os) =>
          if (os.isEmpty) {
            NO_EDGES
          } else {
            reusableIterableThreadLocal.get()
              .bufferProvider(() => os.getBuf)
          }
        case None => NO_EDGES
      }

      val res = f(edges)
      edges match {
        case NO_EDGES =>
          // noop
        case reusable : ReusableIterable[Edge[I,E] @unchecked] =>
          // noop
      }
      res
    }

    override def edges(vId: I): Iterable[Edge[I, E]] = Option(_edges.get(vId)) match {
      case Some(os) =>
        if (os.isEmpty) {
          NO_EDGES
        } else {
          reusableIterableThreadLocal.get()
            .bufferProvider(() => os.getBuf)
        }
      case None => NO_EDGES
    }

    override def releaseEdgesIterable(edgesIterable: Iterable[Edge[I, E]]): Unit = {
      val mutableIterable = mutableReusableIterableThreadLocal.get()
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

    private[this] def serializeEdge(dst : I, value : E, os : DirectByteBufferGrowingOutputStream): Unit = synchronized {
      val kryoOutput = kryoOutputThreadLocal.get()
      val kryo = kryoThreadLocal.get()
      kryoOutput.setOutputStream(os)
      kryo.writeObject(kryoOutput, dst)
      if (!voidOrUnitClass) {
        kryo.writeObject(kryoOutput, value)
      }
      kryoOutput.flush()
//      os.trim()
    }

    override def localNumEdges: Long = numEdges

    override def localNumVertices: Long = _edges.size

    override def localNumActiveVertices : Long = _halted.size

    override def mutableEdges(vId: I): Iterable[Edge[I, E]] = Option(_edges.get(vId)) match {
      case Some(os) =>
        mutableReusableIterableThreadLocal.get()
          .vId(vId)
          .bufferProvider(() => os.getBuf)
      case None =>
        NO_EDGES
    }

    override def addVertex(vertex: I): Unit = edgeStream(vertex)

    override def setVertexValue(vId: I, v: V): Unit = synchronized { _values(vId) = v }
    override def parVertices: ParIterable[I] = throw new UnsupportedOperationException

    override def computeVertices(computation: PregelVertexComputation[I, V, E]): Boolean = {
      throw new UnsupportedOperationException
    }

    override def createQueries(qapComputation : QueryAnswerVertexComputation[I,V,E,_,_,_]): Unit = {
      throw new UnsupportedOperationException
    }

    override def distributeVertices
    (selfWorkerId: WorkerId,
     haltedAsyncSerializer: AsyncSerializer,
     idAsyncSerializer: AsyncSerializer,
     valueAsyncSerializer: AsyncSerializer,
     edgesAsyncSerializer: AsyncSerializer,
     partitioner: VertexPartitioner,
     outputHandler: (ByteBuffer, WorkerId) => Future[ByteBuffer],
     messageStore: MessageStore,
     messagesAsyncSerializer: AsyncSerializer,
     currentOutgoingMessageClass: Class[_])(implicit exeContext : ExecutionContext): Unit = {
      val threadId = ThreadId.getMod(totalPartitions)
      val verticesToBeRemoved = ArrayBuffer.empty[I]
      vertices.toSet.foreach { vId : I =>
        val workerId = partitioner.destination(vId)
        if (selfWorkerId != workerId) {
          verticesToBeRemoved += vId
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
      verticesToBeRemoved.foreach(removeAllFromVertex)
    }
  }
}

class JavaHashMapSerializedVerticesStore[I,V,E]
(protected[this] val clazzI : Class[I],
 protected[this] val clazzE : Class[E],
 protected[this] val clazzV : Class[V],
 numPartitions : Int,
 maxEdgeSize : Int,
 errorReporter : (Throwable) => Unit
) extends VerticesStore[I,V,E] {

  private[this] val forkJoinPool = ExecutionContextUtil.createForkJoinPoolWithPrefix("vertex-partition-workers-", numPartitions, new UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = errorReporter(e)
  })

  private[this] val workers = new Array[PartitionWorker](numPartitions)
  private[this] val partitions = new Array[Partition[I,V,E]](numPartitions)
  for(i <- 0 until numPartitions) {
    workers(i) = new PartitionWorker("vertex-partition-" + i, errorReporter)
    partitions(i) = new Partition[I, V, E](clazzI, clazzE, clazzV, i, numPartitions, maxEdgeSize)
  }

  private[this] var localPartitioner : VertexPartitioner = new VertexHashPartitioner(numPartitions)

  private[this] def partition(vId : I) : Partition[I,V,E] = {
    val partId = localPartitioner.destination(vId)
    partitions(partId)
  }

  override def removeAllFromVertex(vId: I): Unit = {
    val part = partition(vId)
    part.synchronized {
      part.removeAllFromVertex(vId)
    }
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
    part.synchronized{
      part.setHalted(vId, halted)
    }
  }

  override def explicitlyScopedEdges[T](vId: I)(f : Iterable[Edge[I, E]] => T) : T = partition(vId).explicitlyScopedEdges(vId)(f)

  override def edges(vId: I): Iterable[Edge[I, E]] = partition(vId).edges(vId)

  override def releaseEdgesIterable(edgesIterable: Iterable[Edge[I, E]]): Unit = throw new UnsupportedOperationException

  override def addEdge(src: I, dst: I, value: E): Unit = {
    val part = partition(src)
    part.synchronized {
      part.addEdge(src, dst, value)
    }
  }

  override def localNumEdges: Long = partitions.map(_.localNumEdges).sum

  override def localNumVertices: Long = partitions.map(_.localNumVertices).sum

  override def localNumActiveVertices : Long = partitions.map(_.localNumActiveVertices).sum

  override def addVertex(vertex: I): Unit = {
    val part = partition(vertex)
    part.synchronized {
      part.addVertex(vertex)
    }
  }

  override def mutableEdges(vId: I): Iterable[Edge[I, E]] = throw new UnsupportedOperationException

  override def setVertexValue(vId: I, v: V): Unit = {
    val part = partition(vId)
    part.synchronized {
      part.setVertexValue(vId, v)
    }
  }

  override def parVertices: ParIterable[I] = throw new UnsupportedOperationException

  override def computeVertices(computation: PregelVertexComputation[I, V, E]): Boolean = {
    val latch = new CountDownLatch(numPartitions)
    partitions.foreach{ partition =>
      forkJoinPool.submit(new Runnable {
        override def run(): Unit = {
          println(s"${Thread.currentThread().getName} computing")
          try {
            partition.rawEdges.keySet().parallelStream().forEach { vId =>
              val verticesStore = partition
              val simpleVertexInstancePool = new SimplePool[Vertex[I, V, E]](new VertexImpl[I, V, E] {
                override def addEdge(dst: I, e: E): Unit = {
                  verticesStore.addEdge(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
                }
              })
              computation.computeVertex(vId, verticesStore, simpleVertexInstancePool)
            }
          } catch {
            case t : Throwable =>
              val sb = mutable.StringBuilder.newBuilder
              sb.append(t.getMessage)
              sb.append("\n")
              t.getStackTrace.foreach(x => sb.append(x + "\n"))
              println(sb.toString)
          }
          println(s"${Thread.currentThread().getName} finished computing")
          latch.countDown()
        }
      })
    }
    latch.await()
    // TODO abort on exception
    computation.hasHalted
  }

  override def createQueries(qapComputation: QueryAnswerVertexComputation[I,V,E,_,_,_]): Unit = {
    val latch = new CountDownLatch(numPartitions)
    partitions.foreach{ partition =>
      forkJoinPool.submit(new Runnable {
        override def run(): Unit = {
          partition.rawEdges.keySet().parallelStream().forEach{ vId =>
            val verticesStore = partition
            val reusableVertex = new VertexImpl[I,V,E] {
              override def addEdge(dst: I, e: E): Unit = {
                verticesStore.addEdge(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
              }
            }
            qapComputation.vertexQuery(vId, verticesStore, reusableVertex)
          }
          latch.countDown()
        }
      })
    }
    latch.await()
  }

  override def distributeVertices
  (selfWorkerId: WorkerId,
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
              selfWorkerId,
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


