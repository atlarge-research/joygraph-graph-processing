package io.joygraph.core.actor.metrics

import java.util.concurrent.atomic.LongAdder

import akka.actor.Address
import akka.cluster.metrics.NodeMetrics

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

object Network {
  /**
    * Given a NodeMetrics it returns the Cpu data if the nodeMetrics contains
    * necessary cpu metrics.
    * @return if possible a tuple matching the Cpu constructor parameters
    */
  def unapply(nodeMetrics: NodeMetrics): Option[(Address, Long, Long, Long)] = {
    for {
      bytesReceived <- nodeMetrics.metric(NetworkMetrics.BytesReceived)
      bytesSent <- nodeMetrics.metric(NetworkMetrics.BytesSent)
    } yield (nodeMetrics.address, nodeMetrics.timestamp, bytesReceived.value.longValue(), bytesSent.value.longValue())
  }
}

final case class Network(address : Address, timestamp : Long, bytesReceived : Long, bytesSent : Long)