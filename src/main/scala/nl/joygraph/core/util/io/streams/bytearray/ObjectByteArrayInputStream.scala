package nl.joygraph.core.util.io.streams.bytearray

import java.io.{ByteArrayInputStream, DataInputStream}

import nl.joygraph.core.util.io.streams.ObjectInputStream

class ObjectByteArrayInputStream(bytes : Array[Byte]) extends ByteArrayInputStream(bytes) with ObjectInputStream {

  val msgType : Byte = new DataInputStream(this).readByte()
  val counter = new DataInputStream(this).readInt()
  this.pos = 5
}
