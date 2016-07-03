package io.joygraph.core.actor.communication.impl.netty

import java.nio.ByteBuffer

import io.netty.channel.socket.SocketChannel
import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.handler.codec.{ByteToMessageDecoder, LengthFieldBasedFrameDecoder}

class MessageChannelInitializer(maxFrameLength : Int) extends ChannelInitializer[SocketChannel] {

  private[this] val messageReceivedHandler = new MessageReceiveHandler

  def setOnExceptionHandler(reporter : (Channel, Throwable) => Unit) = {
    messageReceivedHandler.setOnExceptionHandler(reporter)
  }

  def setOnReceivedMessage(onMessageReceived : (ByteBuffer) => Any) = {
    messageReceivedHandler.setOnReceivedMessage(onMessageReceived)
  }

  override def initChannel(ch : SocketChannel) = {
    //TODO set max frame length
    val frameDecoder = new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4, true)
    frameDecoder.setCumulator(ByteToMessageDecoder.MERGE_CUMULATOR) // merge
    ch.pipeline()
      .addLast(frameDecoder)
      .addLast(messageReceivedHandler)
  }
}
