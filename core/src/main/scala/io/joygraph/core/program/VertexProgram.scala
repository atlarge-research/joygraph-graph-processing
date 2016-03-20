package io.joygraph.core.program

import com.typesafe.config.Config

object VertexProgram {
  def create[I,V,E,M](clazz : Class[_ <: VertexProgram[I,V,E,M]]) : VertexProgram[I,V,E,M]= {
    clazz.newInstance()
  }
}

trait VertexProgram[I,V,E,M] extends VertexProgramLike[I,V,E,M,M] {

  private[this] var _numVertices : Long = _
  private[this] var _numEdges : Long = _

  /**
    * Load parameters from vertex program
    *
    * @param conf
    */
  def load(conf : Config)
  def totalNumVertices(numVertices : Long) = _numVertices = numVertices
  def totalNumVertices = _numVertices
  def totalNumEdges(numEdges : Long) = _numEdges = numEdges
  def totalNumEdges = _numEdges

}
