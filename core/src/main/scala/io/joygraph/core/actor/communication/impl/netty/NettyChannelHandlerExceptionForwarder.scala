package io.joygraph.core.actor.communication.impl.netty

import io.netty.channel.{Channel, ChannelHandler, ChannelHandlerContext}

trait NettyChannelHandlerExceptionForwarder extends ChannelHandler {
  private[this] var _errorForwarder : (Channel, Throwable) => Unit = _

  def setOnExceptionHandler(reporter : (Channel, Throwable) => Unit) = {
    _errorForwarder = reporter
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    _errorForwarder(ctx.channel(), cause)
  }
}
