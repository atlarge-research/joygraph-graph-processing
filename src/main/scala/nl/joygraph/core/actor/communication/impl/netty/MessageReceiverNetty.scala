package nl.joygraph.core.actor.communication.impl.netty

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelOption}
import nl.joygraph.core.util.PortFinder

import scala.concurrent.{Future, Promise}

class MessageReceiverNetty(onMessageReceived : (ByteBuffer) => Any) {

  val bossGroup = new NioEventLoopGroup()
  val workerGroup = new NioEventLoopGroup()
  var currentChannel : Option[Channel] = None

  val b = new ServerBootstrap()
  b.group(bossGroup, workerGroup)
    .channel(classOf[NioServerSocketChannel])
    .childHandler(new MessageChannelInitializer(onMessageReceived))
    .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
    .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)


  def connect(): Future[Boolean] = {
    val foundPort = PortFinder.findFreePort()
    val promise = Promise[Boolean]
    b.bind(foundPort).addListener(new BindCompleteHandler(promise))
    promise.future
  }

  def shutdown(): Unit = {
    currentChannel match {
      case Some(channel) => channel.close().sync()
      case None =>
    }
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }

  // returns -1 if there is no channel
  def port() : Int = {
    currentChannel match {
      case Some(channel) => channel.localAddress().asInstanceOf[InetSocketAddress].getPort
      case None => -1
    }
  }

  private class BindCompleteHandler(promise : Promise[Boolean]) extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess) {
        promise.success(true)
        currentChannel = Some(future.channel())
      } else {
        // TODO error handling
        future.cause().printStackTrace()
      }
    }
  }
}
