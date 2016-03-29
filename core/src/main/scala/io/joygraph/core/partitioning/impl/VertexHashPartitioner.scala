package io.joygraph.core.partitioning.impl

import com.typesafe.config.Config
import io.joygraph.core.config.JobSettings
import io.joygraph.core.partitioning.VertexPartitioner

class VertexHashPartitioner extends VertexPartitioner {

  def this(numWorkers : Int) {
    this()
    _numWorkers = numWorkers
  }


  def init(conf : Config): Unit = {
    _numWorkers = JobSettings(conf).initialNumberOfWorkers
  }

  override def destination(vId : Any) : Int = vId.hashCode() % _numWorkers
}
