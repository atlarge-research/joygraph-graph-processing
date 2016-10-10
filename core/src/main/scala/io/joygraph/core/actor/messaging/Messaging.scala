package io.joygraph.core.actor.messaging

trait Messaging {

  protected[this] val EMPTY_MESSAGES = Iterable.empty[Any]

  def onBarrier()

  /**
    * Add message to source
    */
  def add[I,M](source: I, message : M)

  def getNext[I,M](source : I) : Iterable[M]

  /**
    * Retrieve messages for source if any
    */
  def get[I,M](source : I) : Iterable[M]

  def remove[I](source : I)

  def removeNext[I](source: I): Unit

  def emptyNextMessages : Boolean
}
