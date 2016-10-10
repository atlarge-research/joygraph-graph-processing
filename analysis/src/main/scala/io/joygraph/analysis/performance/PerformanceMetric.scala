package io.joygraph.analysis.performance

// all in seconds
case class PerformanceMetric(processingTime : Long, makeSpan : Long, machineTime : Long, verticesPerSecond : Long, edgesPerSecond: Long, elasticityOverhead : Long, superStepSumTime : Long) {

  def +=(o : PerformanceMetric) : PerformanceMetric = {
    PerformanceMetric(
      processingTime + o.processingTime,
      makeSpan + o.makeSpan,
      machineTime + o.machineTime,
      verticesPerSecond + o.verticesPerSecond,
      edgesPerSecond + o.edgesPerSecond,
      elasticityOverhead + o.elasticityOverhead,
      superStepSumTime + o.superStepSumTime
    )
  }

  def min(o : PerformanceMetric) : PerformanceMetric = {
    PerformanceMetric(
      math.min(processingTime, o.processingTime),
      math.min(makeSpan, o.makeSpan),
      math.min(machineTime, o.machineTime),
      math.min(verticesPerSecond, o.verticesPerSecond),
      math.min(edgesPerSecond, o.edgesPerSecond),
      math.min(elasticityOverhead, o.elasticityOverhead),
      math.min(superStepSumTime, o.superStepSumTime)
    )
  }

  def max(o : PerformanceMetric) : PerformanceMetric = {
    PerformanceMetric(
      math.max(processingTime, o.processingTime),
      math.max(makeSpan, o.makeSpan),
      math.max(machineTime, o.machineTime),
      math.max(verticesPerSecond, o.verticesPerSecond),
      math.max(edgesPerSecond, o.edgesPerSecond),
      math.max(elasticityOverhead, o.elasticityOverhead),
      math.max(superStepSumTime, o.superStepSumTime)
    )
  }

  def diff(o : PerformanceMetric) : PerformanceMetric = {
    PerformanceMetric(
      math.abs(processingTime - o.processingTime),
      math.abs(makeSpan - o.makeSpan),
      math.abs(machineTime - o.machineTime),
      math.abs(verticesPerSecond - o.verticesPerSecond),
      math.abs(edgesPerSecond - o.edgesPerSecond),
      math.abs(elasticityOverhead - o.elasticityOverhead),
      math.abs(superStepSumTime - o.superStepSumTime)
    )
  }

  def normalizeBy(n : Long) = PerformanceMetric(
    processingTime / n,
    makeSpan / n,
    machineTime / n,
    verticesPerSecond / n,
    edgesPerSecond / n,
    elasticityOverhead / n,
    superStepSumTime / n
  )
}
