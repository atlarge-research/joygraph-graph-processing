package io.joygraph.core.program

import java.io.OutputStream

import scala.reflect._

object ProgramDefinition {
  def create[INPUTFORMAT, I : ClassTag,V : ClassTag,E : ClassTag,M : ClassTag]
  (inputParser : INPUTFORMAT => (I, I, E),
   outputWriter : (Vertex[I,V,E], OutputStream) => Any,
   program : Class[_ <: VertexProgramLike[I,V,E,M,M]]): ProgramDefinition[INPUTFORMAT, I,V,E,M] = new ProgramDefinition[INPUTFORMAT, I,V,E,M](inputParser, outputWriter, program)

}

case class ProgramDefinition[INPUTFORMAT, I : ClassTag,V : ClassTag,E : ClassTag,M : ClassTag](val inputParser : INPUTFORMAT => (I, I, E), val outputWriter : (Vertex[I,V,E], OutputStream) => Any, val program : Class[_ <: VertexProgramLike[I,V,E,M,M]]) {
  val clazzI : Class[I] = classTag[I].runtimeClass.asInstanceOf[Class[I]]
  val clazzV : Class[V] = classTag[V].runtimeClass.asInstanceOf[Class[V]]
  val clazzE : Class[E] = classTag[E].runtimeClass.asInstanceOf[Class[E]]
  val clazzM : Class[M] = classTag[M].runtimeClass.asInstanceOf[Class[M]]
}
