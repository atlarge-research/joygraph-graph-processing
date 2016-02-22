package nl.joygraph.core.util

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.ByteBufferOutput

class ObjectByteBufferOutputStream(val msgType : Byte, maxBufferSize : Int) extends ByteBufferOutput(maxBufferSize, maxBufferSize) with ObjectOutputStream[ByteBuffer] {
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
    this.writeByte(msgType)
    this.writeInt(_counter)
    this.setPosition(originalPos) // THIS DOES NOT SET THE NIO BUFFER POSITION TO originalPos
    this.niobuffer.position(originalPos)
  }

  // hands off the internal byte buffer
  override def handOff(): ByteBuffer = {
    // before handing off, we need to set the limit of our buffer
    // which is what is written as position by the wrapper.
    // so anything between position and limit has to be written.
    this.niobuffer.flip()
    // do not use getByteBuffer, as it does not leave the niobuffer intact.
    this.niobuffer
  }

  override def resetUnderlying(): Unit = {
    this.clear()
    this.setPosition(offset)
    this.niobuffer.position(offset)
  }

}
