package io.joygraph.core.actor.communication.impl.netty

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelOutboundHandlerAdapter

@Sharable
class MessageSenderHandler extends ChannelOutboundHandlerAdapter with NettyChannelHandlerExceptionForwarder  {

}
