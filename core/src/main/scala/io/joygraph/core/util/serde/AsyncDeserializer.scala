package io.joygraph.core.util.serde

import java.util.concurrent.atomic.AtomicLong

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferInputStream

import scala.collection.concurrent.TrieMap

class AsyncDeserializer(kryoFactory : => Kryo) {
  private[this] val _kryos : TrieMap[Int, Kryo] = TrieMap.empty
  val timeSpent = new AtomicLong(0)

  private[this] def kryos(index : Int) = {
    _kryos.getOrElseUpdate(index, kryoFactory)
  }

  def deserialize[T](is : ObjectByteBufferInputStream, index : Int, deserializer : (Kryo, Input) => T)(any : Iterator[T] => Unit) : Unit = {
    val kryo = kryos(index)
    kryo.synchronized{
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
