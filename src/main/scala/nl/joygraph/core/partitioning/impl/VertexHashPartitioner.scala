package nl.joygraph.core.partitioning.impl

import com.typesafe.config.Config
import nl.joygraph.core.partitioning.VertexPartitioner

object VertexHashPartitioner {
  val NUM_TOTAL_WORKERS = "vertexpartitioner.numtotalworkers"
}

class VertexHashPartitioner extends VertexPartitioner {
  private[this] var _numWorkers : Int = _

  def this(numWorkers : Int) {
    this()
    _numWorkers = numWorkers
  }

  def numWorkers(numWorkers : Int) = _numWorkers = numWorkers

  def init(conf : Config): Unit = {
    _numWorkers = conf.getInt(VertexHashPartitioner.NUM_TOTAL_WORKERS)
  }

  override def destination(vId : Any) : Int = vId.hashCode() % _numWorkers
}
