package nl.joygraph.core.actor.communication.impl.netty

import java.nio.ByteBuffer

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.{ByteToMessageDecoder, LengthFieldBasedFrameDecoder, LengthFieldPrepender}

class MessageChannelInitializer(onMessageReceived : (ByteBuffer) => Any) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch : SocketChannel) = {
    val frameDecoder = new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4, true)
    frameDecoder.setCumulator(ByteToMessageDecoder.MERGE_CUMULATOR) // merge
    val frameEncoder = new LengthFieldPrepender(4)
    ch.pipeline()
      .addLast(frameEncoder)
      .addLast(frameDecoder)
      .addLast(new MessageReceiveHandler(onMessageReceived))
  }
}
