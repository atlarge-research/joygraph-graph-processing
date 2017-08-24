package io.joygraph.core.util.collection

import java.lang.reflect.Field
import java.nio.{Buffer, ByteBuffer}

import com.esotericsoftware.kryo.Kryo
import io.netty.util.internal.PlatformDependent
import org.caffinitas.ohc.{CacheSerializer, Eviction, OHCache, OHCacheBuilder}
import sun.misc.Unsafe

import scala.collection.mutable

object OHCWrapper {

  private var addressField : Field = {
    val direct = ByteBuffer.allocateDirect(1)
    var addressFieldTemp : Field = null
    try {
      addressFieldTemp = classOf[Buffer].getDeclaredField("address")
      addressFieldTemp.setAccessible(true)
      if (addressFieldTemp.getLong(ByteBuffer.allocate(1)) != 0) { // A heap buffer must have 0 address.
        addressFieldTemp = null
      }
      else if (addressField.getLong(direct) == 0) { // A direct buffer must have non-zero address.
        addressFieldTemp = null
      }
    } catch {
      case t: Throwable =>
        // Failed to access the address field.
        addressFieldTemp = null
    }
    addressFieldTemp
  }

  private var capacityField : Field = {
    val direct = ByteBuffer.allocateDirect(1)
    var fieldTmp : Field = null
    try {
      fieldTmp = classOf[Buffer].getDeclaredField("capacity")
      fieldTmp.setAccessible(true)
      if (fieldTmp.getLong(ByteBuffer.allocate(1)) != 0) { // A heap buffer must have 0 address.
        fieldTmp = null
      }
      else if (addressField.getLong(direct) == 0) { // A direct buffer must have non-zero address.
        fieldTmp = null
      }
    } catch {
      case t: Throwable =>
        // Failed to access the address field.
        fieldTmp = null
    }
    fieldTmp
  }

  private val UNSAFE : Unsafe = {
    var unsafe : Unsafe = null
    val unsafeField = classOf[Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafe = unsafeField.get(null).asInstanceOf[Unsafe]
    unsafe
  }

  private val ADDRESS_FIELD_OFFSET: Long = UNSAFE.objectFieldOffset(addressField)

  def directBufferAddress(buf : ByteBuffer) : Long = {
    UNSAFE.getLong(buf, ADDRESS_FIELD_OFFSET)
  }

  def directBufferCapacity(buf : ByteBuffer) : Long = {
    buf.capacity()
  }

  def instantiate(address : Long, capacity : Int, buff : ByteBuffer) : ByteBuffer = {
    addressField.setLong(buff, address)
    capacityField.setInt(buff, capacity)
    buff
  }
}

// todo put DirectOutputStream to ByteBuffer

class OHCWrapper[K](kryo : Kryo, keySerializer : CacheSerializer[K]) extends mutable.Map[K, ByteBuffer] {

  private[this] val resuseableByteBuffer = ByteBuffer.allocateDirect(1);

  private[this] val ohCache: OHCache[K, ByteBufferProxy] = OHCacheBuilder
    .newBuilder[K, ByteBufferProxy]()
    .eviction(Eviction.NONE)
    .keySerializer(keySerializer)
    .valueSerializer(new ByteBufferProxySerializer)
    .build()

  override def +=(kv: (K, ByteBuffer)): this.type = {
    if(!ohCache.put(
      kv._1,
      ByteBufferProxy(
        OHCWrapper.directBufferAddress(kv._2), kv._2.capacity()
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

  override def get(key: K) : ByteBuffer = {
    val bbProxy = ohCache.get(key)
    OHCWrapper.instantiate(bbProxy.address, bbProxy.capacity, resuseableByteBuffer)

  }

  override def iterator : Iterator[(K,ByteBuffer)] = new OHCIterator(ohCache)

  class LongCacheSerializer extends CacheSerializer[Long] {
    override def serializedSize(value: Long): Int = 8

    override def serialize(value: Long, buf: ByteBuffer): Unit = buf.putLong(value)

    override def deserialize(buf: ByteBuffer): Long = buf.getLong
  }

  class OHCIterator(private[this] val ohCache : OHCache[K,ByteBufferProxy]) extends Iterator[(K,ByteBuffer)] {
    private[this] val keyIterator : java.util.Iterator[K] = ohCache.keyIterator()
    override def hasNext: Boolean = keyIterator.hasNext

    override def next(): (K, ByteBuffer) = {
      val key = keyIterator.next()
      val bbProxy = ohCache.get(key)
      val value = OHCWrapper.instantiate(bbProxy.address, bbProxy.capacity, resuseableByteBuffer)
      (key, value)
    }
  }

  class ByteBufferProxySerializer extends CacheSerializer[ByteBufferProxy] {
    override def serializedSize(value: ByteBufferProxy): Int = 12

    override def serialize(value: ByteBufferProxy, buf: ByteBuffer): Unit = {
      buf.putLong(value.address)
      buf.putInt(value.capacity)
    }

    override def deserialize(buf: ByteBuffer): ByteBufferProxy = {
      ByteBufferProxy(buf.getLong,  buf.getInt)
    }
  }

  case class ByteBufferProxy(address : Long, capacity : Int) {

  }
}
