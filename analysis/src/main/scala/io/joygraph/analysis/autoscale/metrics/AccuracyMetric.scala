package io.joygraph.analysis.autoscale.metrics

case class AccuracyMetric(u : Double, o : Double, uN : Double, oN : Double, oM : Double) {
  def +=(other : AccuracyMetric): AccuracyMetric = {
    AccuracyMetric(u + other.u, o + other.o, uN + other.uN, oN + other.oN, oM + other.oM)
  }

  def scaleBy(n : Double) : AccuracyMetric = {
    AccuracyMetric(u * n, o * n, uN * n, oN * n, oM * n)
  }

  def normalizeBy(n : Double) : AccuracyMetric = {
    AccuracyMetric(u / n, o / n, uN / n, oN / n, oM / n)
  }
}
