package io.joygraph.core.util.buffers.streams.bytebuffer

import java.nio.ByteBuffer

import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.buffers.streams.ObjectOutputStream
import io.netty.util.internal.PlatformDependent

class ObjectByteBufferOutputStream(val msgType : Byte, maxBufferSize : Int) extends KryoOutput(maxBufferSize, maxBufferSize) with ObjectOutputStream[ByteBuffer] {
  resetOOS()
//  this.varIntsEnabled = false

  /**
    * Write the msg type and the counter to the underlying buffer
    */
  override def writeCounter(): Unit = {
    val originalPos = position()
    this.setPosition(0)  // THIS DOES NOT SET THE NIO BUFFER POSITION TO 0
    this.niobuffer.position(0) // since we want to write at position 0 as we offset it
    // the ByteBuffer order is changed when Kryo is serializing objects.
    // we have to set the order to the default order when writing the counter and msg type.
    this.niobuffer.order(this.order())
    this.writeInt(originalPos - 4)  // -4 for the length bytes
    this.writeInt(0) // reserve for sourceId
    this.writeByte(msgType)
    this.writeInt(_counter)
    this.setPosition(originalPos) // THIS DOES NOT SET THE NIO BUFFER POSITION TO originalPos
    this.niobuffer.position(originalPos)
  }

  def write(byteBuffer: ByteBuffer) = {
    this.niobuffer.put(byteBuffer)
    this.setPosition(niobuffer.position())
  }

  // hands off the internal byte buffer
  override def handOff(): ByteBuffer = {
    // common practice is to let the consumer flip the bytebuffer.
    // do not use getByteBuffer, as it does not leave the niobuffer intact.
    this.niobuffer
  }

  override def resetUnderlying(): Unit = {
    this.clear()
    this.setPosition(offset)
    this.niobuffer.position(offset)
  }

  /**
    * Once called this object will be useless
    */
  def freeMemory() : Unit = {
    PlatformDependent.freeDirectBuffer(this.niobuffer)
  }
}
