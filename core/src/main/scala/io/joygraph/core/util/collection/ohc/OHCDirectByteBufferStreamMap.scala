package io.joygraph.core.util.collection.ohc

import java.nio.ByteBuffer

import io.joygraph.core.util.DirectByteBufferGrowingOutputStream
import io.joygraph.core.util.collection.OHCWrapper

import scala.collection.mutable

class OHCDirectByteBufferStreamMap[K](clazzK : Class[K]) extends mutable.Map[K, DirectByteBufferGrowingOutputStream]{
  private[this] val ohCacheWrapped : OHCWrapper[K] = new OHCWrapper[K](new KryoCacheSerializer[K](clazzK))
  private[this] val reuseableOutputStream : DirectByteBufferGrowingOutputStream = new DirectByteBufferGrowingOutputStream()


  override def +=(kv: (K, DirectByteBufferGrowingOutputStream)) : this.type = {
    ohCacheWrapped += kv._1 -> kv._2.getBufDirect
    this
  }

  override def -=(key: K): this.type = {
    ohCacheWrapped.remove(key) match {
      case Some(value) =>
        reuseableOutputStream.setBuf(value)
        reuseableOutputStream.destroy()
      case None =>
          // noop
    }
    this
  }

  override def get(key: K): Option[DirectByteBufferGrowingOutputStream] = {
    ohCacheWrapped.get(key) match {
      case Some(buf) =>
        reuseableOutputStream.setBuf(buf)
        Some(reuseableOutputStream)
      case None =>
        None
    }
  }

  override def iterator : Iterator[(K, DirectByteBufferGrowingOutputStream)] = {
    new LocalIterator
  }

  class LocalIterator extends Iterator[(K, DirectByteBufferGrowingOutputStream)] {

    private[this] val iterator: Iterator[(K, ByteBuffer)] = ohCacheWrapped.iterator

    override def hasNext: Boolean = iterator.hasNext

    override def next(): (K, DirectByteBufferGrowingOutputStream) = {
      val (key, value) = iterator.next()
      reuseableOutputStream.setBuf(value)
      (key, reuseableOutputStream)
    }
  }
}
