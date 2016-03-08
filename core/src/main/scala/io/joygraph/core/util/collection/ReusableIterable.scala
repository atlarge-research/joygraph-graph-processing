package io.joygraph.core.util.collection

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput

abstract class ReusableIterable[T] extends Iterable[T] {

  protected[this] var _input : ByteBufferInput = _
  protected[this] var _kryo : Kryo = _
  protected[this] val _iterator : ReusableIterator = new ReusableIterator

  def input(input : ByteBufferInput): ReusableIterable[T] = {
    _input = input
    this
  }

  def buffer(byteBuffer : ByteBuffer): ReusableIterable[T] = {
    _input.setBuffer(byteBuffer)
    this
  }

  def kryo(value : Kryo) : ReusableIterable[T] = {
    _kryo = value
    this
  }

  def kryo : Kryo = {
    _kryo
  }

  protected[this] def deserializeObject() : T

  override def iterator: Iterator[T] = {
    _input.setPosition(0) // reset
    _iterator
  }

  class ReusableIterator extends Iterator[T] {
    override def hasNext: Boolean = _input.position() < _input.limit()

    override def next(): T = deserializeObject()
  }
}
