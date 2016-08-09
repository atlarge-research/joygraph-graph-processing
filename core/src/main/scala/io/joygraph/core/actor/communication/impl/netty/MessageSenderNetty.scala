package io.joygraph.core.actor.communication.impl.netty

import java.nio.ByteBuffer
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import io.joygraph.core.actor.communication.MessageSender
import io.joygraph.core.actor.metrics.NetworkMetrics
import io.joygraph.core.util.MessageCounting
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferOutputStream
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}

class MessageSenderNetty(protected[this] val msgCounting: MessageCounting, numThreads : Int = 0) extends MessageSender[Int, ByteBuffer, ByteBuf] {

  private[this] type HostPort = (String, Int)
  private[this] val workerGroupThreadId = new AtomicInteger(0)
  private[this] val workerGroup = new NioEventLoopGroup(numThreads, new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r, "sender-worker-group-" + workerGroupThreadId.incrementAndGet())
  })
  private[this] val _channels : TrieMap[Int, (AtomicInteger, Array[Channel])] = TrieMap.empty
  private[this] val b = new Bootstrap()
  private[this] val messageSenderChannelInitializer = new MessageSenderChannelInitializer
  private[this] var errorReporter : (Channel, Throwable) => Unit = _
  private[this] val RECEIVED_MESSAGE_ID : Byte = -1
  private[this] val RECEIVED_MESSAGE : ByteBuffer = {
    val os = new ObjectByteBufferOutputStream(RECEIVED_MESSAGE_ID, 4096)
    os.writeCounter()
    os.handOff()
  }
  private[this] val RECEIVED_MESSAGE_WRITTER_FLUSHED_LISTENER = new ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (!future.isSuccess) {
        errorReporter(future.channel(), future.cause())
      }
    }
  }

  private[this] def receivedMessageInstance() : ByteBuffer = {
    val singleton = RECEIVED_MESSAGE.duplicate()
    val bb = ByteBuffer.allocate(singleton.position())
    singleton.flip()
    bb.put(singleton)
    bb
  }

  b.group(workerGroup)
  b.channel(classOf[NioSocketChannel])
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
  b.handler(messageSenderChannelInitializer)

  def setOnExceptionHandler(reporter : (Channel, Throwable) => Unit) = {
    errorReporter = reporter
    messageSenderChannelInitializer.setOnExceptionHandler(reporter)
  }

  def connectToAll(destinations : Iterable[(Int, HostPort)]): Iterable[Future[Unit]] = {
    destinations.map{case (destination, hostPort) => connectTo(destination, hostPort)}
  }

  def connectTo(destination : Int, hostPort: HostPort): Future[Unit] = {
    val (host, port) = hostPort
    val promise = Promise[Unit]
    // don't reconnect to connected channel
    _channels.get(destination) match {
      case Some(x) =>
        // noop
        promise.success()
      case None =>
        val numChannels = 4
        val counter = new AtomicInteger(numChannels)
        val channelArr = new Array[Channel](numChannels)
        val channelChooser = new AtomicInteger(-1)
        _channels.put(destination, (channelChooser, channelArr))
        (0 until numChannels).foreach { i =>
          b.connect(host, port).addListener(new ChannelCreateCompleteListener(destination, promise, channelArr, i, counter))
        }
    }
    promise.future
  }

  def disconnectFromAll(destinations : Iterable[Int]) : Iterable[Future[Channel]] = {
    destinations.map(disconnectFrom)
  }

  def disconnectFrom(destination : Int) : Future[Channel] = {
    val promise = Promise[Channel]
    channel(destination).disconnect().addListener(new ChannelDisconnectCompleteListener(destination, promise))
    promise.future
  }

  def shutDown() = workerGroup.shutdownGracefully().sync()

  def closeAllChannels(): Unit = {
    _channels.values.foreach(_._2.foreach(_.close().sync()))
  }

  private[this] def channel(destination : Int) : Channel = {
    val (channelChooser, channels) = _channels(destination)
    val index = channelChooser.incrementAndGet() % channels.length
    channels(Math.abs(index))
  }

  override protected[this] def transform(source : Int, i: ByteBuffer): ByteBuf = {
    // flip
    i.flip()
    // heavy dependence on protocol defined in ObjectOutputStream
    i.putInt(4, source)
    //
    Unpooled.wrappedBuffer(i)
  }

  def sendNoAck(source: Int, destination: Int, payload: ByteBuffer): Future[ByteBuffer] = {
    val byteBuffer = transform(source, payload)
    val promise = Promise[ByteBuffer]
    channel(destination).pipeline().writeAndFlush(byteBuffer).addListener(new MessageWrittenAndFlushedListener(promise, payload))
    promise.future
  }


  override def send(source: Int, destination: Int, payload: ByteBuffer): Future[ByteBuffer] = {
    val byteBuffer = transform(source, payload)
    val promise = Promise[ByteBuffer]
    channel(destination).pipeline().writeAndFlush(byteBuffer).addListener(new MessageWrittenAndFlushedListener(promise, payload))
    msgCounting.incrementSent()
    promise.future
  }

  override def sendAck(source : Int, destination : Int) = {
    val byteBuffer = transform(source, receivedMessageInstance())
    channel(destination).pipeline().writeAndFlush(byteBuffer).addListener(RECEIVED_MESSAGE_WRITTER_FLUSHED_LISTENER)
  }

  private class MessageWrittenAndFlushedListener(promise : Promise[ByteBuffer], payload : ByteBuffer) extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess) {
        NetworkMetrics.bytesSent(payload.limit())
        promise.success(payload)
      } else {
        errorReporter(future.channel(), future.cause())
      }
    }
  }

  private class ChannelDisconnectCompleteListener(destination : Int, p : Promise[Channel]) extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess) {
        _channels.remove(destination)
        p.success(future.channel())
      } else {
        errorReporter(future.channel(), future.cause())
      }
    }
  }

  private class ChannelCreateCompleteListener(destination : Int, p: Promise[Unit], channelArr: Array[Channel], index : Int, counter : AtomicInteger) extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess) {
        channelArr(index) = future.channel()
        if (counter.decrementAndGet() == 0)
          p.success()
      } else {
        errorReporter(future.channel(), future.cause())
      }
    }
  }
}
