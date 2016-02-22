package nl.joygraph.core.util

import java.io.{ByteArrayInputStream, DataInputStream}

class ObjectByteArrayInputStream(bytes : Array[Byte]) extends ByteArrayInputStream(bytes) with ObjectInputStream {

  val msgType : Byte = new DataInputStream(this).readByte()
  val counter = new DataInputStream(this).readInt()
  this.pos = 5
}
