package io.joygraph.core.partitioning.impl

import com.typesafe.config.Config
import io.joygraph.core.config.JobSettings
import io.joygraph.core.partitioning.VertexPartitioner

class VertexHashPartitioner extends VertexPartitioner {
  private[this] var _numWorkers : Int = _

  def this(numWorkers : Int) {
    this()
    _numWorkers = numWorkers
  }

  def numWorkers(numWorkers : Int) = _numWorkers = numWorkers

  def init(conf : Config): Unit = {
    _numWorkers = JobSettings(conf).initialNumberOfWorkers
  }

  override def destination(vId : Any) : Int = vId.hashCode() % _numWorkers
}
