package nl.joygraph.core.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output

import scala.collection.mutable.ArrayBuffer

class AsyncSerializer(msgType : Char, n : Int, kryoFactory : => Kryo, val bufferExceededThreshold : Int = 1024 * 1024) {

  private[this] val buffers : ArrayBuffer[ObjectByteArrayOutputStream] = ArrayBuffer.fill(n)(new ObjectByteArrayOutputStream(msgType))
  private[this] val kryos : ArrayBuffer[Kryo] = ArrayBuffer.fill(n)(kryoFactory)
  private[this] val locks : ArrayBuffer[Object] = ArrayBuffer.fill(n)(new Object)
  private[this] val outputs : ArrayBuffer[Output] = ArrayBuffer.fill(n)(new Output(4096, 4096))

  def serialize(objects : Iterable[_], index : Int)(any : ObjectByteArrayOutputStream => Unit) : Unit = {
    locks(index).synchronized {
      implicit val os = buffers(index)
      val kryo = kryos(index)
      val output = outputs(index)
      output.setOutputStream(os)
      objects.foreach(kryo.writeObject(output, _))
      os.increment()
      output.flush()
      if (os.size() > bufferExceededThreshold) {
        any(os)
      }
    }
  }

  def serialize(o : Any, index : Int)(any : ObjectByteArrayOutputStream => Unit) : Unit = {
    locks(index).synchronized {
      implicit val os = buffers(index)
      val kryo = kryos(index)
      val output = outputs(index)
      output.setOutputStream(os)
      kryo.writeObject(output, o)
      os.increment()
      output.flush()
      if (os.size() > bufferExceededThreshold) {
        any(os)
      }
    }
  }

  def buffer(index : Int): ObjectByteArrayOutputStream = {
    buffers(index)
  }
}
