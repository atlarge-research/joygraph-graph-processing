package io.joygraph.core.actor.messaging

trait Messaging {

  protected[this] val EMPTY_MESSAGES = Iterable.empty[Any]

  def onSuperStepComplete()

  /**
    * Add message to source
    */
  def add[I,M](source: I, message : M)

  /**
    * Retrieve messages for source if any
    */
  def get[I,M](source : I) : Iterable[M]

  def emptyCurrentMessages : Boolean
}
