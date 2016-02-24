package nl.joygraph.core.actor.communication.impl.netty

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import nl.joygraph.core.util.MessageCounting
import org.scalatest.FunSuite
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._

class MessageReceiverSenderNettySuite extends FunSuite with TimeLimitedTests{
  val timeLimit = Span(10, Seconds)

  test("Receiver should bind and shut down") {
    val receiver = new MessageReceiverNetty((b) => ())
    assert(Await.result(receiver.connect(), 10 seconds))
    receiver.shutdown()
  }

  test("Receiver should be able to receive something") {
    val latch = new CountDownLatch(1)
    val receiver = new MessageReceiverNetty((b) => latch.countDown())
    assert(Await.result(receiver.connect(), 10 seconds))
    val port = receiver.port()

    val msgCounter = new Object with MessageCounting
    val sender = new MessageSenderNetty(msgCounter, (b) => ())
    val channel = Await.result(sender.connectTo(0, ("127.0.0.1", port)), 10 seconds)
    Await.result(sender.send(0, 0, ByteBuffer.allocate(1).put(1.asInstanceOf[Byte])), 10 seconds)

    latch.await()
    channel.close().sync()
  }

  test("Receiver should receive same data as written data") {
    val latch = new CountDownLatch(1)
    val receiver = new MessageReceiverNetty((b) =>
      if (b.getInt() === 1234) {
        latch.countDown()
      }
    )
    assert(Await.result(receiver.connect(), 10 seconds))
    val port = receiver.port()

    val msgCounter = new Object with MessageCounting
    val sender = new MessageSenderNetty(msgCounter, (b) => ())
    val channel = Await.result(sender.connectTo(0, ("127.0.0.1", port)), 10 seconds)
    Await.result(sender.send(0, 0, ByteBuffer.allocate(4).putInt(1234)), 10 seconds)
    latch.await()
  }
}
