package io.joygraph.core.actor.communication.impl.netty

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel

@Sharable
class MessageSenderChannelInitializer extends ChannelInitializer[SocketChannel] {

  private[this] val messageSenderHandler = new MessageSenderHandler

  def setOnExceptionHandler(reporter : (Throwable) => Unit) = {
    messageSenderHandler.setOnExceptionHandler(reporter)
  }

  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(messageSenderHandler)
  }
}
