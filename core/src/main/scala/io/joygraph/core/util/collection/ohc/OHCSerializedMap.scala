package io.joygraph.core.util.collection.ohc

import org.caffinitas.ohc.{CloseableIterator, Eviction, OHCache, OHCacheBuilder}

import scala.collection.mutable

class OHCSerializedMap[K, V](clazzK : Class[K], clazzV : Class[V]) extends mutable.Map[K, V]{
  private[this] val ohCache: OHCache[K, V] = OHCacheBuilder
    .newBuilder[K, V]()
    .eviction(Eviction.NONE)
    .keySerializer(new KryoCacheSerializer[K](clazzK))
    .valueSerializer(new KryoCacheSerializer[V](clazzV))
    .build()

  override def +=(kv: (K, V)) : this.type = {
    if(!ohCache.put(kv._1, kv._2)) {
      // grow and retry
      ohCache.setCapacity(ohCache.capacity() >> 1)
      this += kv
    }
    this
  }

  override def -=(key: K): this.type = {
    ohCache.remove(key)
    this
  }

  // override apply to get more performance, but we then aren't strictly compliant to the Map
  override def get(key: K): Option[V] = {
    Option(ohCache.get(key))
  }

  override def iterator : Iterator[(K, V)] = {
    new LocalIterator
  }

  class LocalIterator extends Iterator[(K, V)] {

    private[this] val iterator: CloseableIterator[K] = ohCache.keyIterator

    override def hasNext: Boolean = iterator.hasNext

    override def next(): (K, V) = {
      val key = iterator.next()
      val value = ohCache.get(key)
      (key, value)
    }
  }
}
