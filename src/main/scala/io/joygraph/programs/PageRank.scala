package io.joygraph.programs

import java.util.concurrent.atomic.DoubleAdder

import com.typesafe.config.Config
import io.joygraph.core.program._

object PageRank {
  val DAMPINGFACTOR_CONF_KEY = "dampingFactor"
  val NUMBER_OF_ITERATIONS_CONF_KEY = "numIterations"
  val DANGLINGNODESUM_AGG_NAME = "danglingNodeSum"
}

class PageRank extends HomogeneousVertexProgram[Long, Double, NullClass, Double] with Aggregatable {

  private[this] var dampingFactor : Double = _
  private[this] var numberOfIterations : Int = _
  private[this] var lastDanglingNodeSum : Double = _

  override def preSuperStep(): Unit = {
    lastDanglingNodeSum = previousStepAggregatedValue[Double](PageRank.DANGLINGNODESUM_AGG_NAME)
  }

  override protected[this] def initializeAggregators(): Unit = {
    aggregator[Double](PageRank.DANGLINGNODESUM_AGG_NAME, new Aggregator[Double] {
      private[this] val doubleAdder : DoubleAdder = new DoubleAdder

      override def aggregate(other: Double): Unit = doubleAdder.add(other)

      override def value: Double = doubleAdder.sum()

      override def workerPrepareStep() : Unit = {
        doubleAdder.reset()
      }
      override def masterPrepareStep() : Unit = {
        doubleAdder.reset()
      }
    })
  }

  override def load(conf: Config): Unit = {
    dampingFactor = conf.getDouble(PageRank.DAMPINGFACTOR_CONF_KEY)
    numberOfIterations = conf.getInt(PageRank.NUMBER_OF_ITERATIONS_CONF_KEY)
  }

  override def run(v: Vertex[Long, Double, NullClass], messages: Iterable[Double], superStep: Int)(implicit send: (Double, Long) => Unit, sendAll: (Double) => Unit): Boolean = {
    if (superStep == 0) {
      v.value = 1.0 / totalNumVertices
    } else {
      val sum = lastDanglingNodeSum / totalNumVertices + messages.sum
      v.value = (1.0 - dampingFactor) / totalNumVertices + dampingFactor * sum
    }

    if (superStep < numberOfIterations) {
      if (v.edges.isEmpty) {
        aggregate(PageRank.DANGLINGNODESUM_AGG_NAME, v.value)
      } else {
        sendAll(v.value / v.edges.size.toDouble)
      }
      false
    } else {
      true
    }
  }
}
