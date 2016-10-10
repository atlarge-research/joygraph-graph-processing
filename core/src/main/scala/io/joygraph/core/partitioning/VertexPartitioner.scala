package io.joygraph.core.partitioning

import com.typesafe.config.Config

trait VertexPartitioner extends Serializable {
  protected[this] var _numWorkers : Int = _

  def numWorkers = _numWorkers
  def numWorkers(numWorkers : Int) = _numWorkers = numWorkers
  def init(conf : Config): Unit
  def destination(vId : Any) : Int
}
