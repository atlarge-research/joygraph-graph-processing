package nl.joygraph.core.util.io.streams

trait ObjectInputStream {
  val msgType : Byte
  val counter : Int
}
