package io.joygraph.core.actor.communication.impl.netty

import io.netty.channel.{ChannelHandler, ChannelHandlerContext}

trait NettyChannelHandlerExceptionForwarder extends ChannelHandler {
  private[this] var _errorForwarder : (Throwable) => Unit = _

  def setOnExceptionHandler(reporter : (Throwable) => Unit) = {
    _errorForwarder = reporter
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    _errorForwarder(cause)
  }
}
