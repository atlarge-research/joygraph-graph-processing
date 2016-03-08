package io.joygraph.core.util.buffers.streams.bytearray

import java.io.{ByteArrayInputStream, DataInputStream}

import io.joygraph.core.util.buffers.streams.ObjectInputStream

class ObjectByteArrayInputStream(bytes : Array[Byte]) extends ByteArrayInputStream(bytes) with ObjectInputStream {

  val msgType : Byte = new DataInputStream(this).readByte()
  val counter = new DataInputStream(this).readInt()
  this.pos = 5
}
