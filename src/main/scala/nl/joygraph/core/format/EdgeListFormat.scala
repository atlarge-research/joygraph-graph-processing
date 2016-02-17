package nl.joygraph.core.format

import scala.reflect.ClassTag

abstract class EdgeListFormat[I : ClassTag] extends Format[I] {
  /**
    * Returns an array of 2
    *
    * @param l
    * @return
    */
  override def parse[T >: I : ClassTag](l: String): Array[T] = {
    val s = l.split("\\s")
    val array : Array[T] = new Array[T](2)
    array(0) = stringToI(s(0))
    array(1) = stringToI(s(1))
    array
  }
}
