package nl.joygraph.core.format

import scala.reflect.ClassTag

abstract class Format[I : ClassTag]() {
  def parse[T >: I : ClassTag](l : String) : Array[T]
  def stringToI(vId : String) : I

}
