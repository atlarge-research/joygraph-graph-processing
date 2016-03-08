package io.joygraph.core.actor.vertices.impl

import java.util.concurrent.ConcurrentLinkedQueue

import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.program.Edge

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap

trait TrieMapVerticesStore[I,V,E] extends VerticesStore[I,V,E] {

  private[this] val _halted = TrieMap.empty[I, Boolean]
  private[this] val _vEdges = TrieMap.empty[I, ConcurrentLinkedQueue[Edge[I,E]]]
  private[this] val _vValues = TrieMap.empty[I, V]


  protected[this] def addVertex(vertex : I) : Unit = getCollection(vertex)

  protected[this] def releaseEdgesIterable(edgesIterable : Iterable[Edge[I,E]]) = {
    // noop
  }

  protected[this] def addEdge(src :I, dst : I, value : E): Unit = {
    val neighbours = getCollection(src)
    neighbours.add(Edge(dst, value))
  }

  private[this] def getCollection(vertex : I) : ConcurrentLinkedQueue[Edge[I,E]] = {
    _vEdges.getOrElseUpdate(vertex, new ConcurrentLinkedQueue[Edge[I,E]])
  }

  protected[this] def edges(vId : I) : Iterable[Edge[I,E]] = _vEdges(vId)

  protected[this] def vertices : Iterable[I] = new Iterable[I] {
    override def iterator: Iterator[I] = _vEdges.keysIterator
  }

  protected[this] def halted(vId : I) : Boolean = _halted.getOrElse(vId, false)

  protected[this] def vertexValue(vId : I) : V = _vValues.getOrElse(vId, null.asInstanceOf[V])

  protected[this] def setVertexValue(vId : I, v : V) = _vValues(vId) = v

  protected[this] def setHalted(vId : I, halted : Boolean) =
    if (halted) {
      _halted(vId) = true
    } else {
      _halted.remove(vId)
    }

  protected[this] def numVertices : Int = _vEdges.size
  protected[this] def numEdges : Int = _vEdges.values.map(_.size()).sum
}
