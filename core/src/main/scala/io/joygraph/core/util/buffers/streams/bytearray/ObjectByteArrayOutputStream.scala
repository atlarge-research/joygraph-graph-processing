package io.joygraph.core.util.buffers.streams.bytearray

import java.io.{ByteArrayOutputStream, DataOutputStream}

import io.joygraph.core.util.buffers.streams.ObjectOutputStream

class ObjectByteArrayOutputStream(val msgType : Byte) extends ByteArrayOutputStream with ObjectOutputStream[Array[Byte]] {

  this.count = offset

  override def writeCounter() : Unit = {
    val os = new DataOutputStream(this)
    val originalCount = count
    // set the count to zero so we write at the beginning of the array
    this.count = 0
    os.write(originalCount - 4) // -4 for the length bytes
    os.write(0) // reserve for writeInt
    os.write(msgType)
    os.writeInt(_counter)
    this.count = originalCount
  }

  override def handOff() : Array[Byte] = {
    writeCounter()
    super.toByteArray
  }

  override protected[this] def resetUnderlying(): Unit = {
    super.reset()
    this.count = offset
  }
}
