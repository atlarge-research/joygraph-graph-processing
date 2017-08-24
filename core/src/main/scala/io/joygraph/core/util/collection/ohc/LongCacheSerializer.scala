package io.joygraph.core.util.collection.ohc

import java.nio.ByteBuffer

import org.caffinitas.ohc.CacheSerializer

class LongCacheSerializer extends CacheSerializer[Long] {
  override def serializedSize(value: Long): Int = 8

  override def serialize(value: Long, buf: ByteBuffer): Unit = buf.putLong(value)

  override def deserialize(buf: ByteBuffer): Long = buf.getLong
}
