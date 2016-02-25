package nl.joygraph.core.util

trait ObjectOutputStream[T] {

  val msgType : Byte
  protected[this] var _counter = 0
  // offset 4 for total length
  // offset 4 for source
  // offset 1 for message type
  // offset 4 for the counter
  protected[this] val offset = 13

  def increment(): Unit = {
    _counter += 1
  }

  def counter() = {
    _counter
  }

  /**
    * Write the msg type and the counter to the underlying buffer
    */
  def writeCounter(): Unit

  // hands off the internal byte
  def handOff() : T

  def resetOOS() = {
    resetCounter()
    resetUnderlying()
  }
  protected[this] def resetCounter() : Unit = _counter = 0
  protected[this] def resetUnderlying() : Unit

  def hasElements : Boolean = _counter > 0
}
