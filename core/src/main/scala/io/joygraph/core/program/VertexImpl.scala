package io.joygraph.core.program

import java.io.InputStream

abstract class VertexImpl[I,V,E,M] extends Vertex[I,V,E,M] {
  private[this] var _id : I = _
  private[this] var _value : V = _
  private[this] var _edges : Iterable[Edge[I,E]] = _
  private[this] var _sendFunc : (M,I) => Any = _
  private[this] var _sendAllFunc : (M) => Any = _

  override def id: I = _id

  override def value_=(value: V): Unit = _value = value

  override def edges: Iterable[Edge[I,E]] = _edges

  override def value: V = _value

  /**
    * Load a vertex through bytes
    *
    * @param bytes
    */
  override def load(bytes: Array[Byte]): Unit = ???

  override def load(is: InputStream): Unit = ???


  override def load(id: I, value: V, edges: Iterable[Edge[I,E]]) = {
    _id = id
    _value = value
    _edges = edges
  }

  override def load(id: I, value: V, edges: Iterable[Edge[I,E]], sendFunc : (M, I) => Any , sendAllFunc : (M) => Any) = {
    load(id, value, edges)
    _sendFunc = sendFunc
    _sendAllFunc = sendAllFunc
  }

  override def send(m: M, i: I): Unit = _sendFunc(m, i)

  override def sendAll(m: M): Unit = _sendAllFunc(m)
}
