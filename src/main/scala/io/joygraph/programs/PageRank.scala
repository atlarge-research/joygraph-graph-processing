package io.joygraph.programs

import java.util.concurrent.atomic.DoubleAdder

import com.typesafe.config.Config
import io.joygraph.core.program._

class PageRank extends VertexProgram[Long, Double, NullClass, Double] with Aggregatable {

  private[this] var dampingFactor : Double = _
  private[this] var numberOfIterations : Int = _
  private[this] var lastDanglingNodeSum : Double = _

  override def onSuperStepComplete(): Unit = {
    lastDanglingNodeSum = aggregators()("danglingNodeSum").value.asInstanceOf[Double]
  }

  override def initializeAggregators(): Unit = {
    aggregator[Double]("danglingNodeSum", new Aggregator[Double] {
      private[this] val doubleAdder : DoubleAdder = new DoubleAdder

      override def aggregate(other: Double): Unit = doubleAdder.add(other)

      override def value: Double = doubleAdder.sum()

      override def workerOnStepComplete() : Unit = {
        doubleAdder.reset()
      }
    })
  }

  override def load(conf: Config): Unit = {
    dampingFactor = conf.getDouble("dampingFactor")
    numberOfIterations = conf.getInt("numIterations")
  }

  override def run(v: Vertex[Long, Double, NullClass, Double], messages: Iterable[Double], superStep: Int): Boolean = {
    if (superStep == 0) {
      v.value = 1.0 / totalNumVertices
    } else {
      val sum = lastDanglingNodeSum / totalNumVertices + messages.sum
      v.value = (1.0 - dampingFactor) / totalNumVertices + dampingFactor * sum
    }

    if (superStep < numberOfIterations) {
      if (v.edges.isEmpty) {
        aggregate("danglingNodeSum", v.value)
      } else {
        v.sendAll(v.value / v.edges.size.toDouble)
      }
      false
    } else {
      true
    }
  }
}
