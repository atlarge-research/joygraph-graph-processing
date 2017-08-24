package io.joygraph.core.util.collection.ohc

import java.nio.ByteBuffer

import org.caffinitas.ohc.CacheSerializer

class BooleanCacheSerializer extends CacheSerializer[Boolean] {
  override def serializedSize(value: Boolean): Int = 1

  override def serialize(value: Boolean, buf: ByteBuffer): Unit = {
    val byte : Byte = if (value) 1 else 0
    buf.put(byte)
  }

  override def deserialize(buf: ByteBuffer): Boolean = {
    if (buf.get() == 1) {
      true
    } else {
      false
    }
  }
}
