package io.joygraph.core.actor.metrics

import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryUsage}

import akka.actor.{ActorSystem, Address}
import akka.cluster.Cluster
import akka.cluster.metrics._

object NonHeapMemoryMetrics {
  final val NonHeapMemoryUsed = "non-heap-memory-used"
  final val NonHeapMemoryCommitted = "non-heap-memory-committed"
  final val NonHeapMemoryMax = "non-heap-memory-max"
}

object OffHeapMemoryMetrics {
  final val OffHeapMemoryUsed = "off-heap-memory-used"
  final val OffHeapMemoryMax = "off-heap-memory-max"
}


/**
  * Based on JmxMetricsCollector
  */
class GeneralMetricsCollector(address: Address, decayFactor: Double) extends JmxMetricsCollector(address, decayFactor) {

  private def this(address: Address, settings: ClusterMetricsSettings) = this(address,
      EWMA.alpha(settings.CollectorMovingAverageHalfLife, settings.CollectorSampleInterval))
  def this(system: ActorSystem) = this(Cluster(system).selfAddress, ClusterMetricsExtension(system).settings)

  private val memoryMBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
  private val decayFactorOption = Some(decayFactor)

  def nonHeapMemoryUsage: MemoryUsage = memoryMBean.getNonHeapMemoryUsage

  def nonHeapUsed(heap: MemoryUsage): Option[Metric] = Metric.create(
    name = NonHeapMemoryMetrics.NonHeapMemoryUsed,
    value = heap.getUsed,
    decayFactor = decayFactorOption)

  def nonHeapCommitted(heap: MemoryUsage): Option[Metric] = Metric.create(
    name = NonHeapMemoryMetrics.NonHeapMemoryCommitted,
    value = heap.getCommitted,
    decayFactor = decayFactorOption)

  def nonHeapMax(heap: MemoryUsage): Option[Metric] = Metric.create(
    name = NonHeapMemoryMetrics.NonHeapMemoryMax,
    value = heap.getMax,
    decayFactor = None)

  def offHeapUsed(): Option[Metric] = Metric.create(
    name = OffHeapMemoryMetrics.OffHeapMemoryUsed,
    value = sun.misc.SharedSecrets.getJavaNioAccess.getDirectBufferPool.getMemoryUsed,
    decayFactor = decayFactorOption)

  def offHeapMax(): Option[Metric] = Metric.create(
    name = OffHeapMemoryMetrics.OffHeapMemoryMax,
    value = sun.misc.VM.maxDirectMemory(),
    decayFactor = None)

  def bytesSent() : Option[Metric] = Metric.create(
    name = NetworkMetrics.BytesSent,
    value = NetworkMetrics.getBytesSentAndReset,
    decayFactor = None
  )

  def bytesReceived() : Option[Metric] = Metric.create(
    name = NetworkMetrics.BytesReceived,
    value = NetworkMetrics.getBytesReceivedAndReset,
    decayFactor = None
  )

  override def metrics(): Set[Metric] = {
    val heap = heapMemoryUsage
    val nonHeap = nonHeapMemoryUsage
    Set(systemLoadAverage,
      heapUsed(heap), heapCommitted(heap), heapMax(heap),
      nonHeapUsed(nonHeap), nonHeapCommitted(nonHeap), nonHeapMax(nonHeap),
      offHeapUsed(), offHeapMax(),
      bytesSent(), bytesReceived(),
      processors).flatten
  }
}
