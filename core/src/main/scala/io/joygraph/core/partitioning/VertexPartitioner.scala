package io.joygraph.core.partitioning

trait VertexPartitioner {
  def destination(vId : Any) : Int
}