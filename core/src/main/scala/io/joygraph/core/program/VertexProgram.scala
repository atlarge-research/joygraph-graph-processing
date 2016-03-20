package io.joygraph.core.program

import com.typesafe.config.Config

object VertexProgram {
  def create[I,V,E,M](clazz : Class[_ <: VertexProgram[I,V,E,M]]) : VertexProgram[I,V,E,M]= {
    clazz.newInstance()
  }
}

trait VertexProgram[I,V,E,M] extends VertexProgramLike[I,V,E,M] {

  private[this] var _numVertices : Long = _
  private[this] var _numEdges : Long = _
  private[this] var _sendFunc : (M,I) => Any = _
  private[this] var _sendAllFunc : (Vertex[I,V,E],M) => Any = _

  /**
    * Load parameters from vertex program
    *
    * @param conf
    */
  def load(conf : Config)

  final def messageSenders(sendFunc : (M, I) => Any , sendAllFunc : (Vertex[I,V,E],M) => Any) = {
    _sendFunc = sendFunc
    _sendAllFunc = sendAllFunc
  }

  def totalNumVertices(numVertices : Long) = _numVertices = numVertices
  def totalNumVertices = _numVertices
  def totalNumEdges(numEdges : Long) = _numEdges = numEdges
  def totalNumEdges = _numEdges

  /**
    * @param v
    * @return true if halting, false if not halting
    */
  def run(v: Vertex[I, V, E], messages: Iterable[M], superStep: Int): Boolean
  def send(m: M, i: I): Unit = _sendFunc(m, i)
  def sendAll(v : Vertex[I,V,E], m: M): Unit = _sendAllFunc(v, m)

}
