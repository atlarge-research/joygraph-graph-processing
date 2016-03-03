package nl.joygraph.core.actor.messaging

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput

class ReusableIterable[T] extends Iterable[T] {

  private[this] var _input : ByteBufferInput = _
  private[this] var _kryo : Kryo = _
  private[this] var _clazz : Class[T] = _
  private[this] val _iterator : ReusableIterator = new ReusableIterator

  def clazz(clazz : Class[T]): ReusableIterable[T] = {
    _clazz = clazz
    this
  }

  def input(input : ByteBufferInput): ReusableIterable[T] = {
    _input = input
    this
  }

  def buffer(byteBuffer : ByteBuffer): ReusableIterable[T] = {
    _input.setBuffer(byteBuffer)
    this
  }

  def kryo(kryo : Kryo) : ReusableIterable[T] = {
    _kryo = kryo
    this
  }

  override def iterator: Iterator[T] = _iterator

  class ReusableIterator extends Iterator[T] {
    override def hasNext: Boolean = _input.position() < _input.limit()

    override def next(): T = _kryo.readObject(_input, _clazz)
  }
}
