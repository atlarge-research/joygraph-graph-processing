package nl.joygraph.core.util

import java.util.concurrent.atomic.AtomicLong

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Output, UnsafeMemoryOutput}

import scala.collection.mutable.ArrayBuffer

class AsyncSerializer[T](msgType : Char, n : Int, kryoFactory : => Kryo, val bufferExceededThreshold : Int = 1024 * 1024) {

  private[this] val buffers : ArrayBuffer[ObjectByteArrayOutputStream] = ArrayBuffer.fill(n)(new ObjectByteArrayOutputStream(msgType))
  private[this] val kryos : ArrayBuffer[Kryo] = ArrayBuffer.fill(n)(kryoFactory)
  private[this] val locks : ArrayBuffer[Object] = ArrayBuffer.fill(n)(new Object)
  private[this] val outputs : ArrayBuffer[Output] = ArrayBuffer.fill(n)(new UnsafeMemoryOutput(4096, 4096))
  val timeSpent = new AtomicLong(0)

  def serialize(index : Int, o: T, serializer : (Kryo, Output, T) => Unit)(any : ObjectByteArrayOutputStream => Unit) : Unit = {
    locks(index).synchronized {
      val start = System.currentTimeMillis()
      val os = buffers(index)
      val kryo = kryos(index)
      val output = outputs(index)
      output.setOutputStream(os)
      serializer(kryo, output, o)
      os.increment()
      output.flush()
      val diff = System.currentTimeMillis() - start
      timeSpent.addAndGet(diff)
      if (os.size() > bufferExceededThreshold) {
        any(os)
      }
    }
  }

  def buffer(index : Int): ObjectByteArrayOutputStream = {
    buffers(index)
  }
}
