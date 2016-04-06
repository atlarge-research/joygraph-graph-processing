package io.joygraph.core.actor.vertices.impl

import java.util.concurrent.ConcurrentLinkedQueue

import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.program.Edge

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.collection.parallel.ParIterable

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
}
