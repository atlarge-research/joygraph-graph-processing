package io.joygraph.core.util.buffers.streams.bytebuffer

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.ByteBufferInput
import io.joygraph.core.util.buffers.streams.ObjectInputStream

class ObjectByteBufferInputStream(byteBuffer : ByteBuffer) extends ByteBufferInput(byteBuffer) with ObjectInputStream {
//  this.varIntsEnabled = false
  // position should be set by the provider.
  //  this.niobuffer.position(0)
//  this.setPosition(0)

  override val msgType: Byte = this.readByte()
  override val counter: Int = this.readInt()
}
