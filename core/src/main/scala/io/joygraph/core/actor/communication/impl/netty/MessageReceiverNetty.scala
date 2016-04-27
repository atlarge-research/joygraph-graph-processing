package io.joygraph.core.actor.communication.impl.netty

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import io.joygraph.core.util.net.PortFinder
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelOption}

import scala.concurrent.{Future, Promise}

class MessageReceiverNetty(workerGroupThreads : Int, maxFrameLength : Int) {
  private[this] val bossGroupThreadId = new AtomicInteger(0)
  private[this] val workerGroupThreadId = new AtomicInteger(0)
  val bossGroup = new NioEventLoopGroup(math.max(1, workerGroupThreads >> 1), new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r, "receiver-boss-group-" + bossGroupThreadId.incrementAndGet())
  })
  val workerGroup = new NioEventLoopGroup(workerGroupThreads, new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r, "receiver-worker-group-" + workerGroupThreadId.incrementAndGet())
  }) // worker threads
  var currentChannel : Option[Channel] = None
  val messageChannelInitializer = new MessageChannelInitializer(maxFrameLength)

  val b = new ServerBootstrap()
  b.group(bossGroup, workerGroup)
    .channel(classOf[NioServerSocketChannel])
    .childHandler(messageChannelInitializer)
    .option[ByteBufAllocator](ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true))
    .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
    .childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
    .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)


  def setOnReceivedMessage(onMessageReceived : (ByteBuffer) => Any): Unit = {
    messageChannelInitializer.setOnReceivedMessage(onMessageReceived)
  }

  def setReceiverExceptionReporter(reporter : (Throwable) => Unit) = {
    messageChannelInitializer.setOnExceptionHandler(reporter)
  }

  private[this] def bindToFreePort(p : Promise[Boolean]) = {
    val foundPort = PortFinder.findFreePort()
    b.bind(foundPort).addListener(new BindCompleteHandler(p))
  }

  def connect(): Future[Boolean] = {
    val p = Promise[Boolean]
    bindToFreePort(p)
    p.future
  }

  def shutdown(): Unit = {
    currentChannel match {
      case Some(channel) => channel.close().sync()
      case None =>
    }
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }


  // TODO fix the host/address, it does not return the correct hostname...
//  def address : String = {
//    currentChannel match {
//      case Some(channel) =>
//        channel.localAddress().asInstanceOf[InetSocketAddress].getAddress.toString
//      case None => null
//    }
//  }
//
//  def host : String = {
//    currentChannel match {
//      case Some(channel) =>
//        channel.localAddress().asInstanceOf[InetSocketAddress].getHostName
//      case None => null
//    }
//  }

  // returns -1 if there is no channel
  def port : Int = {
    currentChannel match {
      case Some(channel) => channel.localAddress().asInstanceOf[InetSocketAddress].getPort
      case None => -1
    }
  }

  // TODO add retry limit
  private class BindCompleteHandler(promise : Promise[Boolean]) extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess) {
        // set channel before setting success
        currentChannel = Some(future.channel())
        promise.success(true)
      } else {
        // TODO error handling
        future.cause().printStackTrace()
        bindToFreePort(promise)
      }
    }
  }
}
