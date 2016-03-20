package io.joygraph.core.program

import scala.reflect._

abstract class VertexProgramLike[I: ClassTag,V : ClassTag,E : ClassTag,M: ClassTag] {
  val clazzI : Class[I] = classTag[I].runtimeClass.asInstanceOf[Class[I]]
  val clazzV : Class[V] = classTag[V].runtimeClass.asInstanceOf[Class[V]]
  val clazzE : Class[E] = classTag[E].runtimeClass.asInstanceOf[Class[E]]
  val clazzM : Class[M] = classTag[M].runtimeClass.asInstanceOf[Class[M]]
  def onSuperStepComplete() : Unit = {}
  def onProgramComplete() : Unit = {}
}
