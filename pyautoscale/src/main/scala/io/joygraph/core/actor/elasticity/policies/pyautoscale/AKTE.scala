package io.joygraph.core.actor.elasticity.policies.pyautoscale
import com.typesafe.config.Config

class AKTE extends AutoScalePython("AKTE/v1.0/monitoring") {
  override protected val executableName: String = "AKTE_EaaS.py"

  override def init(policyParams: Config): Unit = {
    super.init(policyParams)
    // AKTE starts up slower for some reason
    Thread.sleep(1000)
  }
}
