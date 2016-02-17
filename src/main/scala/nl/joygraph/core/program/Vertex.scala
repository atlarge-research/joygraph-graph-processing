package nl.joygraph.core.program

import java.io.InputStream

import scala.collection.mutable

object Vertex {

  def create[I,V,E,M](clazzVertex : Class[_ <: Vertex[I,V,E,M]], clazzI : Class[I], clazzV : Class[V], clazzE : Class[E], clazzM : Class[M]) : Vertex[I,V,E,M] = {
    clazzVertex.newInstance()
  }
}

trait Vertex[I,V,E,M] {

  def id : I
  def value : V
  def value_= (value : V) : Unit
  def edges : Iterable[E]
  def send(m : M, i: I)
  def sendAll(m : M)

  def load(id: I, value: V, edges: Iterable[E], messages : mutable.MultiMap[I,M], allMessages : mutable.Buffer[M])
  def load(bytes: Array[Byte])
  def load(is : InputStream)

}
