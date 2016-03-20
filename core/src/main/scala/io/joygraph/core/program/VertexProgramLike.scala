package io.joygraph.core.program

import scala.reflect._

abstract class VertexProgramLike[I: ClassTag,V : ClassTag,E : ClassTag,M_IN: ClassTag, M_OUT : ClassTag] {
  val clazzI : Class[I] = classTag[I].runtimeClass.asInstanceOf[Class[I]]
  val clazzV : Class[V] = classTag[V].runtimeClass.asInstanceOf[Class[V]]
  val clazzE : Class[E] = classTag[E].runtimeClass.asInstanceOf[Class[E]]
  val clazzMIn : Class[M_IN] = classTag[M_IN].runtimeClass.asInstanceOf[Class[M_IN]]
  val clazzMOut : Class[M_OUT] = classTag[M_OUT].runtimeClass.asInstanceOf[Class[M_OUT]]

  private[this] var _sendFunc : (M_OUT,I) => Any = _
  private[this] var _sendAllFunc : (Vertex[I,V,E],M_OUT) => Any = _


  def onSuperStepComplete() : Unit = {}
  def onProgramComplete() : Unit = {}
  /**
    * @param v
    * @return true if halting, false if not halting
    */
  def run(v: Vertex[I, V, E], messages: Iterable[M_IN], superStep: Int): Boolean

  def send(m: M_OUT, i: I): Unit = _sendFunc(m, i)
  def sendAll(v : Vertex[I,V,E], m: M_OUT): Unit = _sendAllFunc(v, m)

  final def messageSenders(sendFunc : (M_OUT, I) => Any , sendAllFunc : (Vertex[I,V,E],M_OUT) => Any) = {
    _sendFunc = sendFunc
    _sendAllFunc = sendAllFunc
  }

}
