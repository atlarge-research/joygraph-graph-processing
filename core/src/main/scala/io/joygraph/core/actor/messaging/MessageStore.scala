package io.joygraph.core.actor.messaging

trait MessageStore[I, M] {

  protected[this] def _handleMessage(index : Int, dstMPair : (I, M))
  protected[this] def messages(dst : I) : Iterable[M]
  protected[this] def releaseMessages(messages : Iterable[M])
  protected[this] def messagesOnSuperStepComplete()
  protected[this] def emptyCurrentMessages : Boolean
}
