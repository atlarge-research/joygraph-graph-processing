package nl.joygraph.core.program

import com.typesafe.config.Config

object VertexProgram {
  def create[I,V,E,M](clazz : Class[_ <: VertexProgram[I,V,E,M]]) : VertexProgram[I,V,E,M]= {
    clazz.newInstance()
  }
}

trait VertexProgram[I,V,E,M] {

  /**
    * Load parameters from vertex program
    *
    * @param conf
    */
  def load(conf : Config)

  /**
    * @param v
    * @return true if halting, false if not halting
    */
  def run(v: Vertex[I, V, E, M], messages: Iterable[M], superStep: Int): Boolean

}
