package io.joygraph.core.actor.communication

import io.joygraph.core.util.MessageCounting

import scala.concurrent.Future

trait MessageSender[D, PI, PO] {
  protected[this] val msgCounting : MessageCounting

  protected[this] def transform(source : D, i : PI) : PO

  def send(source : D, destination : D, payload : PI) : Future[PI]

  def sendAck(source : D, destination : D) : Unit
}
