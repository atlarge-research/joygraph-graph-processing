package nl.joygraph.core.actor.communication.impl

import java.nio.ByteBuffer

import akka.actor.ActorRef
import akka.util.ByteString
import nl.joygraph.core.actor.communication.MessageSender
import nl.joygraph.core.util.MessageCounting

import scala.concurrent.{Future, Promise}

class MessageSenderAkka(protected[this] val msgCounting : MessageCounting) extends MessageSender[ActorRef, ByteBuffer, ByteString] {

  override def send(source: ActorRef, destination: ActorRef, payload: ByteBuffer): Future[ByteBuffer] = {
    val p : Promise[ByteBuffer] = Promise[ByteBuffer]
    implicit val sender = source
    p.success{
      destination ! transform(source, payload)
      msgCounting.incrementSent()
      payload
    }
    p.future
  }

  override def transform(ignored : ActorRef, i: ByteBuffer): ByteString = ByteString(i)
}
