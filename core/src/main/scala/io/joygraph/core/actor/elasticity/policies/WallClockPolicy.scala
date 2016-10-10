package io.joygraph.core.actor.elasticity.policies
import com.typesafe.config.Config
import io.joygraph.core.message.AddressPair

class WallClockPolicy extends DefaultAveragingPolicy[Double] {

  println("WallClockPolicy")

  override def init(policyParams: Config): Unit = {

  }

  override protected[this] def calculateAverage(step: Int, currentWorkers: Iterable[Int]): Option[Double] = Some(averageTimeOfStep(step, currentWorkers))

  override protected[this] def individualWorkerValues(step: Int, currentWorkers: Iterable[Int]): Iterable[(Int, Double)] = workerSuperStepTimes(step, currentWorkers).map{
    case (workerId, time) => workerId -> time.toDouble
  }

  /**
    * Returns 1, if it has a positive impact given the evaluation function, negative impact -1 and there was no decision 0
    */
  override protected[this] def evaluatePreviousDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Int = {
    // check if decision exists
    decision(previousStep(currentStep)) match {
      case Some(x) =>
        if (stepAverage(currentStep).get >= stepAverage(previousStep(currentStep)).get) {
          // runtimes did not change for the better
          -1
        } else {
          1
        }
      case None =>
        0
    }
  }

  // for the running time policy there's always enough information
  override def enoughInformationToMakeDecision(currentStep: Int, currentWorkers: Map[Int, AddressPair]): Boolean = true
}
