package io.joygraph.analysis.autoscale.metrics

case class InstabilityMetric(i : Double, i2 : Double) {
  def +=(other : InstabilityMetric): InstabilityMetric = {
    InstabilityMetric(i + other.i, i2 + other.i2)
  }

  def scaleBy(n : Double) : InstabilityMetric = {
    InstabilityMetric(i * n, i2 * n)
  }

  def normalizeBy(n : Double) : InstabilityMetric = {
    InstabilityMetric(i / n, i2 / n)
  }
}
