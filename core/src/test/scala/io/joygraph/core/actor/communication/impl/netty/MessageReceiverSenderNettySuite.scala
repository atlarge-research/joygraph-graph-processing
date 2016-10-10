package io.joygraph.core.actor.communication.impl.netty

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import io.joygraph.core.util.MessageCounting
import org.scalatest.FunSuite
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class MessageReceiverSenderNettySuite extends FunSuite with TimeLimitedTests{
  val timeLimit = Span(100, Seconds)

  test("Receiver should bind and shut down") {
    val receiver = new MessageReceiverNetty(1, 1024)
    receiver.setOnReceivedMessage((b) => ())
    assert(Await.result(receiver.connect(), 10 seconds))
    receiver.shutdown()
  }

  test("Receiver should be able to receive something") {
    val latch = new CountDownLatch(1)
    val receiver = new MessageReceiverNetty(1, 1024)
    receiver.setOnReceivedMessage((b) => latch.countDown())
    assert(Await.result(receiver.connect(), 10 seconds))
    val port = receiver.port

    val msgCounter = new Object with MessageCounting
    val sender = new MessageSenderNetty(msgCounter)
    val channel = Await.result(sender.connectTo(0, ("127.0.0.1", port)), 10 seconds)
    val someData = ByteBuffer.allocate(10).put(9, 1.asInstanceOf[Byte])
    someData.position(10)
    Await.result(sender.send(0, 0, someData), 10 seconds)

    latch.await()
    sender.closeAllChannels()
  }

  test("Receiver should receive same data as written sender id and data") {
    val latch = new CountDownLatch(1)
    val receiver = new MessageReceiverNetty(1, 1024)
    receiver.setOnReceivedMessage((b) => {
      val senderId = b.getInt()
      val payload = b.getInt()
      if (senderId === 5 && payload === 1234) {
        latch.countDown()
      }
    })
    assert(Await.result(receiver.connect(), 10 seconds))
    val port = receiver.port

    val msgCounter = new Object with MessageCounting
    val sender = new MessageSenderNetty(msgCounter)

    val someData = ByteBuffer.allocate(12)
      .putInt(0, 8) // length of the message (2 ints)
//        .putInt(5, 5) // the sender id
      .putInt(8, 1234) // the message
    someData.position(12)

    val channel = Await.result(sender.connectTo(0, ("127.0.0.1", port)), 10 seconds)
    Await.result(sender.send(5, 0, someData), 10 seconds)
    latch.await()
  }
}
