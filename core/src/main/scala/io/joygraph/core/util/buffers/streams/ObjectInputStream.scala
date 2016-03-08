package io.joygraph.core.util.buffers.streams

trait ObjectInputStream {
  val msgType : Byte
  val counter : Int
}
