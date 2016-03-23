package io.joygraph.core.program

import com.typesafe.config.Config

import scala.reflect._

/**
  * Class for backwards compatibility
  */
abstract class HomogeneousVertexProgram[I,V,E,M : ClassTag] extends NewVertexProgram[I,V,E] {
  private[this] val clazzM : Class[M] = classTag[M].runtimeClass.asInstanceOf[Class[M]]
  override def run() : PartialFunction[Int, SuperStepFunction[I,V,E,M,M]] = {
    case superStep @ _ =>
      new SuperStepFunction[I,V,E,M,M](this, clazzM, clazzM) {
        override def func: (Vertex[I, V, E], Iterable[M]) => Boolean = (v, messages) => {
          implicit val _send : (M,I) => Unit = (m : M, dst : I) => this.send(m, dst)
          implicit val _sendAll : (M) => Unit = (m: M) => this.sendAll(v, m)
          run(v, messages, superStep)
        }
      }
  }
  def run(v: Vertex[I,V,E], messages : Iterable[M], superStep : Int)
         (implicit send : (M,I) => Unit, sendAll : (M) => Unit) : Boolean
}

//object BFS {
//  val UNVISITED : Long = Long.MaxValue
//}
//
//class BFS extends NewVertexProgram[Long, Long, NullClass] {
//
//  var sourceId : Long = -1
//  override def load(conf : Config): Unit = {
//    sourceId = conf.getLong("source_id")
//  }
//
//  override def run(): PartialFunction[Int, SuperStepFunction[Long, Long, NullClass,_, _]] = {
//    case superStep @ 0 =>
//      new SuperStepFunction(this, classOf[Long], classOf[Long]) {
//        override def func: (Vertex[Long, Long, NullClass], Iterable[Long]) => Boolean =
//          (v, m) => {
//            if (v.id == sourceId) {
//              sendAll(v, superStep)
//            } else {
//              v.value = BFS.UNVISITED
//            }
//            true
//        }
//      }
//    case superStep @ _ =>
//      new SuperStepFunction(this, classOf[Long], classOf[Long]) {
//        override def func: (Vertex[Long, Long, NullClass], Iterable[Long]) => Boolean =
//          (v,m) => {
//            if (v.value == BFS.UNVISITED) {
//              v.value = superStep
//              sendAll(v, superStep)
//            }
//            true
//          }
//
//      }
//
//  }
//}

abstract class NewVertexProgram[I,V,E] {

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

  def preSuperStep() : Unit = {}
  def currentSuperStepFunction(superStep : Int) : SuperStepFunction[I,V,E,_,_] = run().lift(superStep).get
  def run(): PartialFunction[Int, SuperStepFunction[I,V,E,_,_]]

  def onSuperStepComplete() : Unit = {}
  def superStepFunction[M1,M2](clazzIn : Class[M1], clazzOut : Class[M2])(x : SuperStepFunction[I,V,E,M1,M2]) : SuperStepFunction[I,V,E,M1,M2] = ???
}

abstract class SuperStepFunction[I,V,E,M1,M2](p : NewVertexProgram[I,V,E], val clazzIn : Class[M1], val clazzOut : Class[M2]) extends ((Vertex[I,V,E], Iterable[Any]) => Boolean) {

  private[this] var _sendFunc : (M2,I) => Any = _
  private[this] var _sendAllFunc : (Vertex[I,V,E],M2) => Any = _

  def func : ((Vertex[I,V,E], Iterable[M1]) => Boolean)

  override def apply(v: Vertex[I,V,E], messages: Iterable[Any]): Boolean = func(v,messages.asInstanceOf[Iterable[M1]])

  final def messageSenders(sendFunc : (M2, I) => Any , sendAllFunc : (Vertex[I,V,E],M2) => Any) = {
    _sendFunc = sendFunc
    _sendAllFunc = sendAllFunc
  }

  def send(m: M2, i: I): Unit = _sendFunc(m, i)
  def sendAll(v : Vertex[I,V,E], m: M2): Unit = _sendAllFunc(v, m)


}