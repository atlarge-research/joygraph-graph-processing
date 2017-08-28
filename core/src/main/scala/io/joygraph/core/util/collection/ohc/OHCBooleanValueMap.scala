package io.joygraph.core.util.collection.ohc

import org.caffinitas.ohc.{Eviction, OHCache, OHCacheBuilder}

import scala.collection.mutable

object OHCBooleanValueMap {

}

class OHCBooleanValueMap[K](private[this] val clazz: Class[K]) extends mutable.Map[K, Boolean] {
  private[this] val ohCache: OHCache[K, Boolean] = OHCacheBuilder
    .newBuilder[K, Boolean]()
    .eviction(Eviction.NONE)
    .keySerializer(new KryoCacheSerializer(clazz))
    .valueSerializer(new BooleanCacheSerializer)
    .build()


  override def +=(kv: (K, Boolean)) : this.type = {
    if(!ohCache.put(kv._1, kv._2)) {
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

  override def get(key: K) : Option[Boolean] = {
    Option(ohCache.get(key))
  }

  override def iterator : Iterator[(K, Boolean)] = {
    new KeyBooleanIterator
  }

  class KeyBooleanIterator extends Iterator[(K,Boolean)]{
    private[this] val keyIterator = ohCache.keyIterator()

    override def hasNext: Boolean = keyIterator.hasNext

    override def next(): (K, Boolean) = {
      val key = keyIterator.next()
      val value = ohCache.get(key)
      (key, value)
    }
  }
}
