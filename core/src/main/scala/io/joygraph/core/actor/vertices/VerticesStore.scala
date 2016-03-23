package io.joygraph.core.actor.vertices

import io.joygraph.core.program.Edge


trait VerticesStore[I,V,E] {

  protected[this] def addVertex(vertex : I)
  protected[this] def addEdge(src :I, dst : I, value : E)
  protected[this] def edges(vId : I) : Iterable[Edge[I,E]]
  protected[this] def mutableEdges(vId : I) : Iterable[Edge[I,E]]
  protected[this] def releaseEdgesIterable(edgesIterable : Iterable[Edge[I,E]])
  protected[this] def vertices : Iterable[I]
  protected[this] def halted(vId : I) : Boolean
  protected[this] def vertexValue(vId : I) : V
  protected[this] def setVertexValue(vId : I, v : V)
  protected[this] def setHalted(vId : I, halted : Boolean)
  protected[this] def numVertices : Int
  protected[this] def numEdges : Int
}
