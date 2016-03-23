package io.joygraph.core.program

import java.io.InputStream

abstract class VertexImpl[I,V,E] extends Vertex[I,V,E] {
  private[this] var _id : I = _
  private[this] var _value : V = _
  private[this] var _edges : Iterable[Edge[I,E]] = _
  private[this] var _mutableEdges : Iterable[Edge[I,E]] = _


  override def id: I = _id

  override def value_=(value: V): Unit = _value = value

  override def edges: Iterable[Edge[I,E]] = _edges

  override def mutableEdges : Iterable[Edge[I,E]] = _mutableEdges

  override def value: V = _value

  /**
    * Load a vertex through bytes
    *
    * @param bytes
    */
  override def load(bytes: Array[Byte]): Unit = ???

  override def load(is: InputStream): Unit = ???

  override def load(id: I, value: V, edges: Iterable[Edge[I,E]], mutableEdges : Iterable[Edge[I,E]]) = {
    _id = id
    _value = value
    _edges = edges
    _mutableEdges = mutableEdges
  }

}
