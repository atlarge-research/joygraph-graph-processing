package io.joygraph.core.actor.vertices.impl.serialized

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.actor.vertices.impl.serialized.OpenHashMapSerializedVerticesStore.Partition
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
import scala.collection.parallel.ParIterable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object OpenHashMapSerializedVerticesStore {

  protected[OpenHashMapSerializedVerticesStore] class Partition[I,V,E]
  (override protected[this] val clazzI: Class[I],
   override protected[this] val clazzE: Class[E],
   override protected[this] val clazzV: Class[V],
   val partitionIndex : Int,
   val totalPartitions : Int,
   maxEdgeSize : Int
  ) extends VerticesStore[I,V,E] {
    private[this] val _halted : scala.collection.mutable.Map[I, Boolean] = mutable.OpenHashMap.empty
    private[this] val _edges : scala.collection.mutable.Map[I, DirectByteBufferGrowingOutputStream] = mutable.OpenHashMap.empty
    protected[OpenHashMapSerializedVerticesStore] val rawEdges = _edges
    private[this] val _values :  scala.collection.mutable.Map[I, V] = mutable.OpenHashMap.empty
    private[this] val NO_EDGES = Iterable.empty[Edge[I,E]]
    // TODO inject kryo
    private[this] val kryo = new Kryo()
    private[this] val kryoOutput = new KryoOutput(maxEdgeSize, maxEdgeSize)
    private[this] var numEdges = 0L

    private[this] val reusableIterablePool = new SimplePool({
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
    })

    private[this] val reusableIterable = new ReusableIterable[Edge[I,E]] {
      private[this] val reusableEdge : Edge[I,E] = Edge[I,E](null.asInstanceOf[I], null.asInstanceOf[E])
      override protected[this] def deserializeObject(): Edge[I, E] = {
        reusableEdge.dst = _kryo.readObject(_input, clazzI)
        if (!voidOrUnitClass) {
          reusableEdge.e = _kryo.readObject(_input, clazzE)
        }
        reusableEdge
      }
    }.input(new ByteBufferInput(maxEdgeSize))
      .kryo(kryo)


    private[this] val mutableIterable = {
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

    def explicitlyScopedEdges[T](vId: I)(f : Iterable[Edge[I, E]] => T) : T = {
      val edges = _edges.get(vId) match  {
        case Some(os) =>
          if (os.isEmpty) {
            NO_EDGES
          } else {
            reusableIterablePool.borrow()
              .bufferProvider(() => os.getBuf)
          }
        case None => NO_EDGES
      }

      val res = f(edges)
      edges match {
        case NO_EDGES =>
          // noop
        case reusable : ReusableIterable[Edge[I,E] @unchecked] =>
          reusableIterablePool.release(reusable)
      }
      res
    }

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
      if (!voidOrUnitClass) {
        kryo.writeObject(kryoOutput, value)
      }
      kryoOutput.flush()
//      os.trim()
    }

    override def localNumEdges: Long = numEdges

    override def localNumVertices: Long = _edges.keys.size

    override def localNumActiveVertices : Long = _halted.size

    override def countActiveVertices(m: MessageStore, clazzM: Class[_]): Long = {
      vertices.count(vId => !halted(vId) || m.nextMessages(vId, clazzM).nonEmpty)
    }

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

    override def computeVertices(computation: PregelVertexComputation[I, V, E]): Boolean = {
      val verticesStore = this
      val simpleVertexInstancePool = new SimplePool[Vertex[I,V,E]](new VertexImpl[I,V,E] {
        override def addEdge(dst: I, e: E): Unit = {
          verticesStore.addEdge(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
        }
      })
      vertices.foreach(computation.computeVertex(_, verticesStore, simpleVertexInstancePool))
      computation.hasHalted
    }

    override def createQueries(qapComputation : QueryAnswerVertexComputation[I,V,E,_,_,_]): Unit = {
      val verticesStore = this
      val reusableVertex = new VertexImpl[I,V,E] {
        override def addEdge(dst: I, e: E): Unit = {
          verticesStore.addEdge(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
        }
      }
      vertices.foreach(qapComputation.vertexQuery(_, verticesStore, reusableVertex))
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
      vertices.foreach { vId : I =>
        val workerId = partitioner.destination(vId)
        if (selfWorkerId != workerId) {
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
    }

    override def removeDistributedVerticesEdgesAndMessages(selfWorkerId: WorkerId, partitioner: VertexPartitioner): Unit = {
      vertices
        .filter(vId => partitioner.destination(vId) != selfWorkerId)
        .foreach(removeAllFromVertex)
    }

    def clearAll(): Unit = {
      _halted.clear()
      _edges.clear()
      _values.clear()
      numEdges = 0
    }

    def addNumEdges(n : Long) : Unit = {
      numEdges += n
    }
  }
}

class OpenHashMapSerializedVerticesStore[I,V,E]
(protected[this] val clazzI : Class[I],
 protected[this] val clazzE : Class[E],
 protected[this] val clazzV : Class[V],
 numPartitions : Int,
 maxEdgeSize : Int,
 errorReporter : (Throwable) => Unit
) extends VerticesStore[I,V,E] {

  private[this] val workers = new Array[PartitionWorker](numPartitions)
  private[this] val _partitions = new Array[Partition[I,V,E]](numPartitions)
  for(i <- 0 until numPartitions) {
    workers(i) = new PartitionWorker("vertex-partition-" + i, errorReporter)
    _partitions(i) = new Partition[I, V, E](clazzI, clazzE, clazzV, i, numPartitions, maxEdgeSize)
  }

  private[this] var localPartitioner : VertexPartitioner = new VertexHashPartitioner(numPartitions)

  def partitions : Array[Partition[I,V,E]] = _partitions
  private[this] def partition(vId : I) : Partition[I,V,E] = {
    val partId = localPartitioner.destination(vId)
    _partitions(partId)
  }
//
//  def internalBalance(): Unit = {
//    // check if there's deviation between partitions
//    val averageNumVertices = partitions.map(_.localNumVertices).sum / numPartitions
//    val reHashPartitionerBuilder = new ReHashPartitioner.Builder()
//      .partitioner(localPartitioner)
//    val tooManyVertices = partitions.zipWithIndex
//      .filter {
//        case (part, index) =>
//          part.localNumVertices > averageNumVertices
//      }.map {
//      case (part, index) =>
//        index -> (part.localNumVertices - averageNumVertices).toDouble / averageNumVertices.toDouble
//    }
//    val tooLittleVertices = partitions.zipWithIndex.filter {
//      case (part, index) =>
//        part.localNumVertices < averageNumVertices
//    }.map{
//      case (part, index) =>
//       index -> (1.0 - (part.localNumVertices.toDouble / averageNumVertices.toDouble))
//    }
//    tooLittleVertices.foreach{
//      case (index, fraction) =>
//        fraction -
//    }
//  }

  override def removeAllFromVertex(vId: I): Unit = {
    val part = partition(vId)
    part.synchronized {
      part.removeAllFromVertex(vId)
    }
  }

  override def halted(vId: I): Boolean = partition(vId).halted(vId)

  override def vertices: Iterable[I] = {
    new Iterable[I] {
      override def iterator: Iterator[I] = _partitions.map(_.vertices.toIterator).reduce(_ ++ _)
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

  override def localNumEdges: Long = _partitions.map(_.localNumEdges).sum

  override def localNumVertices: Long = _partitions.map(_.localNumVertices).sum

  override def localNumActiveVertices : Long = _partitions.map(_.localNumActiveVertices).sum

  override def countActiveVertices(m: MessageStore, clazzM: Class[_]): Long = {
    val aLong = new AtomicLong(0)
    workers.zipWithIndex.map {
      case (worker, index) =>
        val promise = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            val numVertices = _partitions(index).countActiveVertices(m, clazzM)
            aLong.addAndGet(numVertices)
            promise.success()
          }
        })
        promise.future
    }.foreach(Await.ready(_, Duration.Inf))
    aLong.get()
  }

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
     workers.zipWithIndex.map {
      case (worker, index) =>
        val promise = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            _partitions(index).computeVertices(computation)
            promise.success()
          }
        })
        promise.future
    }.foreach(Await.ready(_, Duration.Inf))
    // TODO abort on exception
    computation.hasHalted
  }

  override def createQueries(qapComputation: QueryAnswerVertexComputation[I,V,E,_,_,_]): Unit = {
    workers.zipWithIndex.map {
      case (worker, index) =>
        val promise = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            _partitions(index).createQueries(qapComputation)
            promise.success()
          }
        })
        promise.future
    }.foreach(Await.ready(_, Duration.Inf))
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
            _partitions(index).distributeVertices(
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


  override def removeDistributedVerticesEdgesAndMessages(selfWorkerId: WorkerId, partitioner: VertexPartitioner): Unit = {
    workers.zipWithIndex.map {
      case (worker, index) =>
        val promise = Promise[Unit]
        worker.execute(new Runnable {
          override def run(): Unit = {
            _partitions(index).removeDistributedVerticesEdgesAndMessages(selfWorkerId, partitioner)
            promise.success()
          }
        })
        promise.future
    }.foreach(Await.ready(_, Duration.Inf))
  }
//
//  def getRawEdges(vId : I): Option[DirectByteBufferGrowingOutputStream] = {
//    val part = partition(vId)
//    part.rawEdges.get(vId)
//  }
//
//  def setRawEdges(vId : I, rawEdgesStream : DirectByteBufferGrowingOutputStream) = {
//    val part = partition(vId)
//    part.rawEdges(vId) = rawEdgesStream
//  }

  override def addAll(other: VerticesStore[I, V, E]): Unit = {
    other match {
      case otherStore: OpenHashMapSerializedVerticesStore[I, V, E] =>
        // partitions correspond
        otherStore.partitions.zipWithIndex.foreach{
          case (otherPart, index) =>
            otherPart.rawEdges.foreach{
              case (vId, rawEdges) =>
                partitions(index).rawEdges(vId) = rawEdges
                partitions(index).setVertexValue(vId, otherPart.vertexValue(vId))
                partitions(index).setHalted(vId, otherPart.halted(vId))
            }
            partitions(index).addNumEdges(otherPart.localNumEdges)
            otherPart.clearAll()
        }
      case _ =>
    }
  }
}

