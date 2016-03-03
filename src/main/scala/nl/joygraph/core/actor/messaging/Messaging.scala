package nl.joygraph.core.actor.messaging

/**
  * @tparam I source vertex type
  * @tparam M message type
  */
trait Messaging[I,M] {

  protected[this] val EMPTY_MESSAGES = Iterable.empty[M]

  def onSuperStepComplete()

  /**
    * Add message to source
    */
  def add(source: I, message : M)

  /**
    * Retrieve messages for source if any
    */
  def get(source : I) : Iterable[M]

  def emptyCurrentMessages : Boolean
}
