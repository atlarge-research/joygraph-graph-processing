package io.joygraph.core.partitioning

import com.typesafe.config.Config

trait VertexPartitioner {
  def init(conf : Config): Unit
  def destination(vId : Any) : Int
}
