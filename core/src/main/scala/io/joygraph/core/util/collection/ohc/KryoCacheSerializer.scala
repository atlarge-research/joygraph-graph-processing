package io.joygraph.core.util.collection.ohc

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutputStream}
import io.joygraph.core.util.buffers.KryoOutput
import org.caffinitas.ohc.CacheSerializer

class KryoCacheSerializer[V](private[this] val clazz: Class[V]) extends CacheSerializer[V] {

  private[this] val kryo : Kryo = new Kryo
  private[this] val kryoOutput : KryoOutput = new KryoOutput(4096, 4096)
  private[this] val input : ByteBufferInput = new ByteBufferInput()

  override def serializedSize(value: V): Int = {
    kryoOutput.clear()
    kryo.writeObject(kryoOutput, value)
    kryoOutput.position()
  }

  override def serialize(value: V, buf: ByteBuffer): Unit = {
    kryoOutput.clear()
    kryoOutput.setOutputStream(new ByteBufferOutputStream(buf))
    kryo.writeObject(kryoOutput, value)
    kryoOutput.flush()
  }

  override def deserialize(buf: ByteBuffer): V = {
    input.setBuffer(buf)
    kryo.readObject(input, clazz)
  }
}
