package io.joygraph.core.program

@deprecated
trait RequestResponse[I,V,E, Req, Res] {

  val reqClazz : Class[Req]
  val resClazz : Class[Res]

  private[this] var _requestFunc : (Any, I) => Any = _

  def setRequestFunc(f : (Any, I) => Any) = {
    _requestFunc = f
  }

  def request(r : Req, dst : I) : Res = _requestFunc(r, dst).asInstanceOf[Res]

  final def responseProxy(v : Vertex[I,V,E], r : Any) : Res = {
    r match {
      case request : Req @unchecked =>
        response(v, request)
      case _ =>
        throw new IllegalArgumentException("Wrong class")
    }
  }

  def response(v : Vertex[I,V,E], r : Req) : Res
}
