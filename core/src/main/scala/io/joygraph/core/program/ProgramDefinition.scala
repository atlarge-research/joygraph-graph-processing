package io.joygraph.core.program

import java.io.OutputStream

import scala.reflect._

case class ProgramDefinition[INPUTFORMAT, I : ClassTag,V : ClassTag,E : ClassTag]
(edgeParser : INPUTFORMAT => (I, I, E),
 vertexParser : INPUTFORMAT => I,
 outputWriter : (Vertex[I,V,E], OutputStream) => Any,
 program : Class[_ <: NewVertexProgram[I,V,E]]) {
  val clazzI : Class[I] = classTag[I].runtimeClass.asInstanceOf[Class[I]]
  val clazzV : Class[V] = classTag[V].runtimeClass.asInstanceOf[Class[V]]
  val clazzE : Class[E] = classTag[E].runtimeClass.asInstanceOf[Class[E]]
}
