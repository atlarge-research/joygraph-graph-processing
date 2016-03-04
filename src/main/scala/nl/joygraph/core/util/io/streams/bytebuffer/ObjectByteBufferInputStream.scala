package nl.joygraph.core.util.io.streams.bytebuffer

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.ByteBufferInput
import nl.joygraph.core.util.io.streams.ObjectInputStream

class ObjectByteBufferInputStream(byteBuffer : ByteBuffer) extends ByteBufferInput(byteBuffer) with ObjectInputStream {
//  this.varIntsEnabled = false
  // position should be set by the provider.
  //  this.niobuffer.position(0)
//  this.setPosition(0)

  override val msgType: Byte = this.readByte()
  override val counter: Int = this.readInt()
}
