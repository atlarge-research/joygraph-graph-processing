package nl.joygraph.core.util

import java.util.concurrent.atomic.AtomicLong

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input

import scala.collection.mutable.ArrayBuffer

class AsyncDeserializerNew[T](msgType : Char, n : Int, kryoFactory : => Kryo) {
  private[this] val kryos : ArrayBuffer[Kryo] = ArrayBuffer.fill(n)(kryoFactory)
  private[this] val locks : ArrayBuffer[Object] = ArrayBuffer.fill(n)(new Object)

  val timeSpent = new AtomicLong(0)

  def deserialize(is : ObjectByteBufferInputStream, index : Int, deserializer : (Kryo, Input) => T)(any : Iterator[T] => Unit) : Unit = {
    Predef.assert(is.msgType == msgType)
    locks(index).synchronized{
      val kryo = kryos(index)
      val objects = new Iterator[T] {
        var numObjects = 0

        override def hasNext: Boolean = numObjects < is.counter

        override def next(): T = {
          numObjects += 1
          val start = System.currentTimeMillis()

          val res = deserializer(kryo, is)
          val diff = System.currentTimeMillis() - start
          timeSpent.addAndGet(diff)
          res
        }
      }
      any(objects)
    }
  }
}
