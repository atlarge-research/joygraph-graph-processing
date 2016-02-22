package nl.joygraph.core.actor.communication

import nl.joygraph.core.util.MessageCounting

import scala.concurrent.Future

trait MessageSender[D, PI, PO] {
  protected[this] val msgCounting : MessageCounting

  protected[this] def transform(i : PI) : PO

  def send(source : D, destination : D, payload : PI) : Future[PI]
}
