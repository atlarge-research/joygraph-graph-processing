package io.joygraph.core.program

import com.typesafe.config.Config

import scala.reflect._

/**
  * Class for backwards compatibility
  */
abstract class HomogeneousVertexProgram[I,V,E,M : ClassTag] extends NewVertexProgram[I,V,E] {
  private[this] val clazzM : Class[M] = classTag[M].runtimeClass.asInstanceOf[Class[M]]
  override def run() : PartialFunction[Int, PregelSuperStepFunction[I,V,E,M,M]] = {
    case superStep @ _ =>
      new PregelSuperStepFunction[I,V,E,M,M](this, clazzM, clazzM) {
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
  def newInstance(conf : Config) : NewVertexProgram[I,V,E] = {
    val instance = this.getClass.newInstance()
    instance.load(conf)
    instance.totalNumVertices(this.totalNumVertices)
    instance.totalNumEdges(this.totalNumEdges)
    instance.asInstanceOf[NewVertexProgram[I,V,E]]
  }

  def preSuperStep() : Unit = {}
  def currentSuperStepFunction(superStep : Int) : SuperStepFunction[I,V,E] = run().lift(superStep).get
  def run(): PartialFunction[Int, SuperStepFunction[I,V,E]]

  def onSuperStepComplete() : Unit = {}
}

abstract class PregelSuperStepFunction[I,V,E,M1,M2](p : NewVertexProgram[I,V,E], val clazzIn : Class[M1], val clazzOut : Class[M2]) extends ((Vertex[I,V,E], Iterable[Any]) => Boolean) with SuperStepFunction[I,V,E] {

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

abstract class QueryAnswerProcessSuperStepFunction[I,V,E,S,G,M](p : NewVertexProgram[I,V,E], val clazzS : Class[S], val clazzG : Class[G], val classM : Class[M]) extends SuperStepFunction[I,V,E] {
  def query(v : Vertex[I,V,E], messages : Iterable[M]) : Iterable[(I,S)]
  def answer(v : Vertex[I,V,E], query : S) : G
  def process(v : Vertex[I,V,E], data : Iterable[G]) : Boolean
}

abstract class QueryAnswerProcessSuperStepFunctionNoPregelMessage[I,V,E,S,G](p : NewVertexProgram[I,V,E], clazzS : Class[S], clazzG : Class[G]) extends QueryAnswerProcessSuperStepFunction[I,V,E,S,G, Unit](p, clazzS, clazzG, classOf[Unit]) {
  final def query(v : Vertex[I,V,E], messages : Iterable[Unit]) : Iterable[(I,S)] = {
    query(v)
  }

  def query(v : Vertex[I,V,E]) : Iterable[(I,S)]

}