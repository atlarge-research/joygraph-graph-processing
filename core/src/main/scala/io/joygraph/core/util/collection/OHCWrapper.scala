package io.joygraph.core.util.collection

import java.lang.reflect.{Constructor, Field}
import java.nio.{Bits, ByteBuffer}

import io.joygraph.core.util.ByteBufferUtil
import io.netty.util.internal.PlatformDependent
import org.caffinitas.ohc.{CacheSerializer, Eviction, OHCache, OHCacheBuilder}

import scala.collection.mutable

object OHCWrapper {

  def directBufferAddress(buf : ByteBuffer) : Long = {
    // TODO move netty dependencies out
    PlatformDependent.directBufferAddress(buf)
  }

  def directBufferCapacity(buf : ByteBuffer) : Long = {
    buf.capacity()
  }

  def instantiate(baseAndSize : Array[Long], address : Long, capacity : Int) : ByteBuffer = {
    ByteBufferUtil.BBCONSTRUCTOR.newInstance(Long.box(address), Int.box(capacity), baseAndSize)
  }

  def instantiate(address : Long, capacity : Int, limit : Int, position: Int, buff : ByteBuffer) : ByteBuffer = {
    ByteBufferUtil.ADDRESS_FIELD.setLong(buff, address)
    ByteBufferUtil.CAPACITY_FIELD.setInt(buff, capacity)
    ByteBufferUtil.LIMIT_FIELD.setInt(buff, limit)
    ByteBufferUtil.POSITION_FIELD.setInt(buff, position)
    // TODO add baseandsize
    // we need it to destroy
    //
    buff
  }
}

class OHCWrapper[K](keySerializer : CacheSerializer[K]) extends mutable.Map[K, ByteBuffer] {

  private[this] val resuseableByteBuffer = ByteBuffer.allocateDirect(1)

  private[this] val ohCache: OHCache[K, ByteBufferProxy] = OHCacheBuilder
    .newBuilder[K, ByteBufferProxy]()
    .eviction(Eviction.NONE)
    .keySerializer(keySerializer)
    .valueSerializer(new ByteBufferProxySerializer)
    .build()

  override def +=(kv: (K, ByteBuffer)): this.type = {
    val (key, byteBuffer) = kv
    if(!ohCache.put(
      key,
      ByteBufferProxy(
        OHCWrapper.directBufferAddress(byteBuffer),
        byteBuffer.capacity(),
        byteBuffer.limit(),
        byteBuffer.position()
      )
    )) {
      // grow and retry
      ohCache.setCapacity(ohCache.capacity() >> 1)
      this += kv
    }
    this
  }

  override def -=(key: K) : this.type = {
    ohCache.remove(key)
    this
  }

  // override def apply if we want extra performance
  override def get(key: K) : Option[ByteBuffer] = {
    Option(ohCache.get(key)) match {
      case Some(bbProxy) =>
        Some(OHCWrapper.instantiate(bbProxy.address, bbProxy.capacity, bbProxy.limit, bbProxy.position, resuseableByteBuffer))
      case None => None
    }
  }

  override def iterator : Iterator[(K,ByteBuffer)] = new OHCIterator(ohCache)

  class OHCIterator(private[this] val ohCache : OHCache[K,ByteBufferProxy]) extends Iterator[(K,ByteBuffer)] {
    private[this] val keyIterator : java.util.Iterator[K] = ohCache.keyIterator()
    override def hasNext: Boolean = keyIterator.hasNext

    override def next(): (K, ByteBuffer) = {
      val key = keyIterator.next()
      val bbProxy = ohCache.get(key)
      val value = OHCWrapper.instantiate(bbProxy.address, bbProxy.capacity, bbProxy.limit, bbProxy.position, resuseableByteBuffer)
      (key, value)
    }
  }

  class ByteBufferProxySerializer extends CacheSerializer[ByteBufferProxy] {
    override def serializedSize(value: ByteBufferProxy): Int = 20

    override def serialize(value: ByteBufferProxy, buf: ByteBuffer): Unit = {
      buf.putLong(value.address)
      buf.putInt(value.capacity)
      buf.putInt(value.limit)
      buf.putInt(value.position)
    }

    override def deserialize(buf: ByteBuffer): ByteBufferProxy = {
      val address = buf.getLong()
      val capacity = buf.getInt()
      val limit = buf.getInt()
      val position = buf.getInt
      ByteBufferProxy(address, capacity, limit, position)
    }
  }

  case class ByteBufferProxy(address : Long, capacity : Int, limit : Int, position : Int)
}
