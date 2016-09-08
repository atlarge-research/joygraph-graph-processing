package io.joygraph.core.actor.elasticity.policies.pyautoscale

class Hist extends AutoScalePython("Hist/v1.0/monitoring") {
  override protected val executableName: String = "Hist_EaaS.py"
}
