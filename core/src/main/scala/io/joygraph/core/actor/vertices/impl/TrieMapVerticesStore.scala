package io.joygraph.core.actor.vertices.impl

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

import io.joygraph.core.actor.PregelVertexComputation
import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program.{Edge, Vertex, VertexImpl}
import io.joygraph.core.util.serde.AsyncSerializer
import io.joygraph.core.util.{SimplePool, ThreadId}

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParIterable
import scala.concurrent.{ExecutionContext, Future}

class TrieMapVerticesStore[I,V,E]
(protected[this] val clazzI : Class[I],
protected[this] val clazzE : Class[E],
protected[this] val clazzV : Class[V]) extends VerticesStore[I,V,E] {

  private[this] val _halted = TrieMap.empty[I, Boolean]
  private[this] val _vEdges = TrieMap.empty[I, ConcurrentLinkedQueue[Edge[I,E]]]
  private[this] val _vValues = TrieMap.empty[I, V]

  def addVertex(vertex : I) : Unit = getCollection(vertex)

  def releaseEdgesIterable(edgesIterable : Iterable[Edge[I,E]]) = {
    // noop
  }

  def addEdge(src :I, dst : I, value : E): Unit = {
    val neighbours = getCollection(src)
    neighbours.add(Edge(dst, value))
  }

  private[this] def getCollection(vertex : I) : ConcurrentLinkedQueue[Edge[I,E]] = {
    _vEdges.getOrElseUpdate(vertex, new ConcurrentLinkedQueue[Edge[I,E]])
  }

  def explicitlyScopedEdges[T](vId: I)(f : Iterable[Edge[I, E]] => T) : T = f(edges(vId))

  def edges(vId : I) : Iterable[Edge[I,E]] = _vEdges(vId)

  // TODO mutable edges are not functional
  def mutableEdges(vId : I) : Iterable[Edge[I,E]] = _vEdges(vId)

  def parVertices : ParIterable[I] = {
    _vEdges.par.keys
  }

  def vertices : Iterable[I] = new Iterable[I] {
    override def iterator: Iterator[I] = _vEdges.keysIterator
  }

  def halted(vId : I) : Boolean = _halted.getOrElse(vId, false)

  def vertexValue(vId : I) : V = _vValues.getOrElse(vId, null.asInstanceOf[V])

  def setVertexValue(vId : I, v : V) = _vValues(vId) = v

  def setHalted(vId : I, halted : Boolean) =
    if (halted) {
      _halted(vId) = true
    } else {
      _halted.remove(vId)
    }

  def localNumVertices : Int = _vEdges.size
  def localNumEdges : Int = _vEdges.values.map(_.size()).sum

  def removeAllFromVertex(vId : I): Unit = {
    _halted.remove(vId)
    _vEdges.remove(vId)
    _vValues.remove(vId)
  }

  override def computeVertices(computation: PregelVertexComputation[I, V, E]): Boolean = {
    val verticesStore = this
    val simpleVertexInstancePool = new SimplePool[Vertex[I,V,E]](new VertexImpl[I,V,E] {
      override def addEdge(dst: I, e: E): Unit = {
        verticesStore.addEdge(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
      }
    })
    parVertices.foreach(vId => computation.computeVertex(vId, verticesStore, simpleVertexInstancePool))
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
   currentOutgoingMessageClass: Class[_])(implicit exeContext: ExecutionContext): Unit = {

    val verticesToBeRemoved = TrieMap.empty[ThreadId, ArrayBuffer[I]]
    parVertices.foreach{ vId =>
      val workerId = partitioner.destination(vId)
      if (newWorkersMap(workerId)) {
        val threadId = ThreadId.getMod(8) // TODO
        ???
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
