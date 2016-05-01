package io.joygraph.core.actor.metrics

import java.util.concurrent.atomic.LongAdder

object NetworkMetrics {
  final val BytesSent = "network-bytes-sent"
  final val BytesReceived = "network-bytes-received"

  private val received : LongAdder = new LongAdder
  private val sent : LongAdder = new LongAdder

  def getBytesReceivedAndReset : Long = {
    received.sumThenReset()
  }
  def getBytesSentAndReset : Long = {
    sent.sumThenReset()
  }

  def bytesReceived(bytes : Long) = {
    received.add(bytes)
  }

  def bytesSent(bytes : Long) = {
    sent.add(bytes)
  }


}
