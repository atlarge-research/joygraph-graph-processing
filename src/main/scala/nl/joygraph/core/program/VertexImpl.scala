package nl.joygraph.core.program

import java.io.InputStream

import scala.collection.mutable

class VertexImpl[I,V,E,M] extends Vertex[I,V,E,M] {
  private[this] var _id : I = _
  private[this] var _value : V = _
  private[this] var _edges : Iterable[Edge[I,E]] = _
  private[this] var _messages : mutable.MultiMap[I,M] = _
  private[this] var _allMessages : mutable.Buffer[M] = _

//  private[this] def resetMessages(): Unit = {
//    _messages = new mutable.OpenHashMap[I, ArrayBuffer[M]] with mutable.MultiMap[I, M]
//    _allMessages = new mutable.ArrayBuffer[M]
//  }

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

  override   def load(id: I, value: V, edges: Iterable[Edge[I,E]], messages : mutable.MultiMap[I,M], allMessages : mutable.Buffer[M]) = {
    _id = id
    _value = value
    _edges = edges
    _messages = messages
    _allMessages = allMessages
  }

  override def send(m: M, i: I): Unit = _messages.addBinding(i, m)

  override def sendAll(m: M): Unit = _allMessages += m
}
