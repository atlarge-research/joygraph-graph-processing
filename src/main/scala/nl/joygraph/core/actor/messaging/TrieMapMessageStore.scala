package nl.joygraph.core.actor.messaging

trait TrieMapMessageStore[I,M] extends MessageStore[I,M]{
  private[this] val messaging : Messaging[I,M] = new TrieMapMessaging[I,M]

  override protected[this] def _handleMessage(index : Int, dstMPair : (I, M)) = {
    val (dst, m) = dstMPair
    messaging.add(dst, m)
  }

  override protected[this] def messages(dst : I) : Iterable[M] = {
    messaging.get(dst)
  }

  override protected[this] def messagesOnSuperStepComplete(): Unit = {
    messaging.onSuperStepComplete()
  }

  override protected[this] def emptyCurrentMessages : Boolean = {
    messaging.emptyCurrentMessages
  }

}
