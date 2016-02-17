package nl.joygraph.core.util

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

trait MessageCounting {
  private[this] val messagesReceived = new AtomicInteger(0)
  private[this] val messagesSent = new AtomicInteger(0)
  private[this] val sentLastMessage = new AtomicBoolean(false)

  protected[this] def resetSentReceived() = {
    messagesReceived.set(0)
    messagesSent.set(0)
    sentLastMessage.set(false)
  }

  protected[this] def incrementReceived(): Unit = {
    messagesReceived.incrementAndGet()
  }

  protected[this] def incrementSent(): Unit = {
    messagesSent.incrementAndGet()
  }

  protected[this] def numMessagesSent = messagesSent.get
  protected[this] def numMessagesReceived = messagesReceived.get

  protected[this] def sendingComplete() : Unit = {
    sentLastMessage.set(true)
  }

  protected[this] def doneAllSentReceived : Boolean = {
    sentLastMessage.get && messagesReceived.get == messagesSent.get
  }


}
