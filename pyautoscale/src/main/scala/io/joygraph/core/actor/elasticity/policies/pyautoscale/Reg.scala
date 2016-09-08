package io.joygraph.core.actor.elasticity.policies.pyautoscale

class Reg extends AutoScalePython("Reg/v1.0/monitoring") {
  override protected val executableName: String = "Reg_EaaS.py"
}
