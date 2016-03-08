package io.joygraph.core.program

import java.io.InputStream

object Vertex {

  def create[I,V,E,M](clazzVertex : Class[_ <: Vertex[I,V,E,M]], clazzI : Class[I], clazzV : Class[V], clazzE : Class[E], clazzM : Class[M]) : Vertex[I,V,E,M] = {
    clazzVertex.newInstance()
  }
}

trait Vertex[I,V,E,M] {

  def id : I
  def value : V
  def value_= (value : V) : Unit
  def edges : Iterable[Edge[I,E]]
  def send(m : M, i: I)
  def sendAll(m : M)
  def addEdge(dst : I, e : E)
  def load(id: I, value: V, edges: Iterable[Edge[I,E]], sendFunc : (M, I) => Any , sendAllFunc : (M) => Any)
  def load(id: I, value: V, edges: Iterable[Edge[I,E]])
  def load(bytes: Array[Byte])
  def load(is : InputStream)

}
