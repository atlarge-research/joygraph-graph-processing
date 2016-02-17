package nl.joygraph.core.util

import java.io.{ByteArrayOutputStream, DataOutputStream}

class ObjectByteArrayOutputStream(val msgType : Char) extends ByteArrayOutputStream {
  private[this] var _counter = 0
  // offset 4 for the counter and 1 for message type
  private[this] val offset = 5
  this.count = offset

  def increment(): Unit = {
    _counter += 1
  }

  def counter() = {
    _counter
  }

  def writeCounter() : Array[Byte] = {
    val os = new DataOutputStream(this)
    val originalCount = count
    this.count = 0
    os.write(msgType)
    os.writeInt(_counter)
    this.count = originalCount
    super.toByteArray
  }

  override def toByteArray: Array[Byte] = {
    writeCounter()
  }

  override def reset(): Unit = {
    super.reset()
    this.count = offset
    _counter = 0
  }

}
