package io.joygraph.core.actor.elasticity.policies.pyautoscale

class ConPaaS extends AutoScalePython("ConPaaS/v1.0/monitoring") {
  override protected val executableName: String = "ConPaaS_EaaS.py"
}
