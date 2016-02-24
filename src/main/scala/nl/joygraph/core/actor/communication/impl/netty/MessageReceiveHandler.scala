package nl.joygraph.core.actor.communication.impl.netty

import java.nio.ByteBuffer

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

@Sharable
class MessageReceiveHandler(onMessageReceived : (ByteBuffer) => Any) extends SimpleChannelInboundHandler[ByteBuf] {

  override def channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf): Unit = {
    // since DirectByteBuffer is using a merged CUMULATOR, nioBuffer() returns a single PooledDirectByteBuffer
    onMessageReceived(msg.nioBuffer())
  }
}
