package nl.joygraph.core.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.pool.{KryoFactory, KryoPool}

class AsyncPooledDeserializer[T](msgType : Char, n : Int, kryoFactory : => Kryo) {

  private[this] val pool = new KryoPool.Builder(new KryoFactory() {
    override def create(): Kryo = {
      kryoFactory
    }
  }).softReferences().build()

  def deserialize(is : ObjectByteArrayInputStream, index : Int, deserializer : (Kryo, Input) => T)(any : Iterator[T] => Unit) : Unit = {
    Predef.assert(is.msgType == msgType)
    val kryo = pool.borrow()
    val input = new Input(is)
    val objects = new Iterator[T] {
      var numObjects = 0

      override def hasNext: Boolean = numObjects < is.counter

      override def next(): T = {
        numObjects += 1
        deserializer(kryo, input)
      }
    }
    any(objects)
    pool.release(kryo)
  }
}