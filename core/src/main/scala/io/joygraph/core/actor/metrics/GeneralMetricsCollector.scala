package io.joygraph.core.actor.metrics

import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryUsage}

import akka.actor.{ActorSystem, Address}
import akka.cluster.Cluster
import akka.cluster.metrics._
import org.hyperic.sigar.SigarProxy

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
  * Based on SigarMetricsCollector
  */
class GeneralMetricsCollector(address: Address, decayFactor: Double, sigar : SigarProxy) extends JmxMetricsCollector(address, decayFactor) {

  import StandardMetrics._
  import org.hyperic.sigar.CpuPerc

  private val memoryMBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
  private val decayFactorOption = Some(decayFactor)

  def this(address: Address, settings: ClusterMetricsSettings, sigar: SigarProxy) =
    this(address,
      EWMA.alpha(settings.CollectorMovingAverageHalfLife, settings.CollectorSampleInterval),
      sigar)

  def this(address: Address, settings: ClusterMetricsSettings) =
    this(address, settings, {
      val sigarProvider = DefaultSigarProvider(settings)
      val sigarInstance = sigarProvider.createSigarInstance
      sigarInstance
    })

  /**
    * This constructor is used when creating an instance from configured FQCN
    */
  def this(system: ActorSystem) = this(Cluster(system).selfAddress, {
    val metricsExtension = ClusterMetricsExtension(system)
    val metricsExtensionSettings = metricsExtension.settings
    metricsExtensionSettings
  })

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
    value = NetworkMetrics.getBytesSent,
    decayFactor = None
  )

  def bytesReceived() : Option[Metric] = Metric.create(
    name = NetworkMetrics.BytesReceived,
    value = NetworkMetrics.getBytesReceived,
    decayFactor = None
  )

  override def systemLoadAverage: Option[Metric] = Metric.create(
    name = SystemLoadAverage,
    value = sigar.getLoadAverage()(0).asInstanceOf[Number],
    decayFactor = None)

  def cpuCombined(cpuPerc: CpuPerc): Option[Metric] = Metric.create(
    name = CpuCombined,
    value = cpuPerc.getCombined.asInstanceOf[Number],
    decayFactor = decayFactorOption)

  def cpuStolen(cpuPerc: CpuPerc): Option[Metric] = Metric.create(
    name = CpuStolen,
    value = cpuPerc.getStolen.asInstanceOf[Number],
    decayFactor = decayFactorOption)

  def cpuIdle(cpuPerc: CpuPerc): Option[Metric] = Metric.create(
    name = CpuIdle,
    value = cpuPerc.getIdle.asInstanceOf[Number],
    decayFactor = decayFactorOption)

  override def close(): Unit = SigarProvider.close(sigar)

  override def metrics(): Set[Metric] = {
    val heap = heapMemoryUsage
    val nonHeap = nonHeapMemoryUsage
    val cpuPerc = sigar.getCpuPerc
    Set(systemLoadAverage,
      cpuCombined(cpuPerc), cpuStolen(cpuPerc),
      heapUsed(heap), heapCommitted(heap), heapMax(heap),
      nonHeapUsed(nonHeap), nonHeapCommitted(nonHeap), nonHeapMax(nonHeap),
      offHeapUsed(), offHeapMax(),
      bytesSent(), bytesReceived(),
      processors).flatten
  }
}
