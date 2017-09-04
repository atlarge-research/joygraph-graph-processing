package io.joygraph.core.util

import java.nio.ByteBuffer

import org.caffinitas.ohc.{CacheSerializer, Eviction, OHCacheBuilder}
import org.scalatest.FunSuite

import scala.collection.mutable

class CrapTest extends FunSuite {
  test("testAss") {
    var start = System.currentTimeMillis()

    val ohCache = OHCacheBuilder.newBuilder()
      .keySerializer(new CacheSerializer[Int] {
        override def serializedSize(value: Int) = 4

        override def serialize(value: Int, buf: ByteBuffer) = buf.putInt(value)

        override def deserialize(buf: ByteBuffer) = buf.getInt
      })
      .valueSerializer(new CacheSerializer[Int] {
        override def serializedSize(value: Int) = 4

        override def serialize(value: Int, buf: ByteBuffer) = buf.putInt(value)

        override def deserialize(buf: ByteBuffer) = buf.getInt
      })
      .eviction(Eviction.NONE)
      .capacity(100000)
      .build()

    for (i <- 0 until 10000000) {

      if (!ohCache.put(i, i)) {
        ohCache.setCapacity(ohCache.capacity() + 100000)
        ohCache.put(i, i)
      }
    }

    var z : Long = 0
    for (i <- 0 until 10000000) {
      z += ohCache.get(i)
    }

    println(s"${System.currentTimeMillis() - start}: $z")

//
//    start = System.currentTimeMillis()
//    val derp = mutable.OpenHashMap.empty[Int, Int]
//
//    for (i <- 0 until 10000000) {
//      derp.put(i, i)
//    }
//
//    z = 0
//    for (i <- 0 until 10000000) {
//      z += derp(i)
//    }
//    println(s"${System.currentTimeMillis() - start}: $z")

  }
}
