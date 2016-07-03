package io.joygraph.core.actor.communication.impl.netty

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{Channel, ChannelInitializer}

@Sharable
class MessageSenderChannelInitializer extends ChannelInitializer[SocketChannel] {

  private[this] val messageSenderHandler = new MessageSenderHandler

  def setOnExceptionHandler(reporter : (Channel, Throwable) => Unit) = {
    messageSenderHandler.setOnExceptionHandler(reporter)
  }

  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(messageSenderHandler)
  }
}
