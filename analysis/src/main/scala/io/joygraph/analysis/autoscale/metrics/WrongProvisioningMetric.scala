package io.joygraph.analysis.autoscale.metrics

case class WrongProvisioningMetric(u : Double, o : Double) {
  def +=(other : WrongProvisioningMetric): WrongProvisioningMetric = {
    WrongProvisioningMetric(u + other.u, o + other.o)
  }

  def scaleBy(n : Double) : WrongProvisioningMetric = {
    WrongProvisioningMetric(u * n, o * n)
  }

  def normalizeBy(n : Double) : WrongProvisioningMetric = {
    WrongProvisioningMetric(u / n, o / n)
  }
}
