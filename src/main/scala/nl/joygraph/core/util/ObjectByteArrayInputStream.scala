package nl.joygraph.core.util

import java.io.{ByteArrayInputStream, DataInputStream}

class ObjectByteArrayInputStream(bytes : Array[Byte]) extends ByteArrayInputStream(bytes) {

  val msgType = new DataInputStream(this).read()
  val counter = new DataInputStream(this).readInt()
  this.pos = 5
}
