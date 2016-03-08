package io.joygraph.core.actor.communication.impl.netty

import java.nio.ByteBuffer

import io.joygraph.core.actor.communication.MessageSender
import io.joygraph.core.util.MessageCounting
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}

class MessageSenderNetty(protected[this] val msgCounting: MessageCounting) extends MessageSender[Int, ByteBuffer, ByteBuf] {

  private[this] type HostPort = (String, Int)
  private[this] val workerGroup = new NioEventLoopGroup()
  private[this] val _channels : TrieMap[Int, Channel] = TrieMap.empty
  private[this] val b = new Bootstrap()

  b.group(workerGroup)
  b.channel(classOf[NioSocketChannel])
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
  b.handler(new MessageSenderChannelInitializer)

  def connectToAll(destinations : Iterable[(Int, HostPort)]): Iterable[Future[Channel]] = {
    destinations.map{case (destination, hostPort) => connectTo(destination, hostPort)}
  }

  def connectTo(destination : Int, hostPort: HostPort): Future[Channel] = {
    val (host, port) = hostPort
    val promise = Promise[Channel]
    b.connect(host, port).addListener(new ChannelCreateCompleteListener(destination, promise))
    promise.future
  }

  def shutDown() = workerGroup.shutdownGracefully().sync()

  def closeAllChannels(): Unit = {
    _channels.values.foreach(_.close().sync())
  }

  private[this] def channel(destination : Int) : Channel = {
    _channels(destination)
  }

  override protected[this] def transform(source : Int, i: ByteBuffer): ByteBuf = {
    // flip
    i.flip()
    // heavy dependence on protocol defined in ObjectOutputStream
    i.putInt(4, source)
    //
    Unpooled.wrappedBuffer(i)
  }

  override def send(source: Int, destination: Int, payload: ByteBuffer): Future[ByteBuffer] = {
    val byteBuffer = transform(source, payload)
    val promise = Promise[ByteBuffer]
    channel(destination).pipeline().writeAndFlush(byteBuffer).addListener(new MessageWrittenAndFlushedListener(promise, payload))
    msgCounting.incrementSent()
    promise.future
  }

  private class MessageWrittenAndFlushedListener(promise : Promise[ByteBuffer], payload : ByteBuffer) extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess) {
        promise.success(payload)
      } else {
        future.cause().printStackTrace()
        // TODO handle error
      }
    }
  }

  private class ChannelCreateCompleteListener(destination : Int, p: Promise[Channel]) extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess) {
        _channels.put(destination, future.channel())
        p.success(future.channel())
      } else {
        // TODO error handling
        future.cause().printStackTrace()
      }
    }
  }
}
