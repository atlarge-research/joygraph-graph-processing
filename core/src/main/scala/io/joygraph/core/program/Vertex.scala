package io.joygraph.core.program

import java.io.InputStream

trait Vertex[I,V,E] {

  def id : I
  def value : V
  def value_= (value : V) : Unit
  def edges : Iterable[Edge[I,E]]
  def mutableEdges : Iterable[Edge[I,E]]
  def addEdge(dst : I, e : E)
  def load(id: I, value: V, edges: Iterable[Edge[I,E]])
  def load(id: I, value: V, edges: Iterable[Edge[I,E]], mutableEdges : Iterable[Edge[I,E]])
  def load(bytes: Array[Byte])
  def load(is : InputStream)
}
