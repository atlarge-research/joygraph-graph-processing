package io.joygraph.core.program

trait VertexProgramLike[I,V,E,M] {

  def onSuperStepComplete() : Unit = {}
}
