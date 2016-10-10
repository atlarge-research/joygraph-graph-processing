package io.joygraph.core.actor.elasticity.policies
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Result, Shrink}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner

abstract class DefaultAveragingPolicy[T](implicit num: Fractional[T]) extends AveragingPolicy[T]{

  override protected[this] def positiveEvaluationAction(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
//    baseEvaluationAction(currentStep, currentWorkers, currentPartitioner, maxNumWorkers)
    None
  }

  override protected[this] def negativeEvaluationAction(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
    // revert decision
    decision(currentStep - 1) match {
      case Some(result) =>
        result match {
          case Shrink(workersToRemove, partitioner) =>
            Some(Grow(workersToRemove, vertexPartitioner(currentStep - 1)))
          case Grow(workersToAdd, partitioner) =>
            Some(Shrink(workersToAdd, vertexPartitioner(currentStep - 1)))
        }
      case None =>
        None // nothing to revert
    }
  }

}
