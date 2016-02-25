package nl.joygraph.core.actor.communication.impl.netty

import java.nio.ByteBuffer

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

@Sharable
class MessageReceiveHandler extends SimpleChannelInboundHandler[ByteBuf] {

  private[this] var _onMessageReceived : (ByteBuffer) => Any = _

  /**
    * Should only be called when NO messages are expected for the previous function
    * @param onMessageReceived function which consumes a byteBuffer
    */
  def setOnReceivedMessage(onMessageReceived : (ByteBuffer) => Any): Unit = synchronized {
    _onMessageReceived = onMessageReceived
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf): Unit = {
    // since DirectByteBuffer is using a merged CUMULATOR, nioBuffer() returns a single Pooled(Unsafe)DirectByteBuffer
    // TODO maybe return Future[Any]
    _onMessageReceived(msg.nioBuffer()) // NOTE: has to be blocking otherwise it'll be released prematurely by SimpleChannelInboundHandler
  }
}
