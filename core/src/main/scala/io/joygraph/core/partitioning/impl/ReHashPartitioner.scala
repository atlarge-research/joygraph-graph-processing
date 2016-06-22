package io.joygraph.core.partitioning.impl

import com.typesafe.config.Config
import io.joygraph.core.partitioning.VertexPartitioner

import scala.collection.mutable

class ReHashPartitioner(partitioner : VertexPartitioner) extends VertexPartitioner {

  private[this] val extraMapping = mutable.OpenHashMap.empty[Int, Int]

  override def init(conf: Config): Unit = {
    //
  }

  def distribute(from : Int, to : Int) = {
    extraMapping += from -> to
  }

  override def destination(vId: Any): Int = {
    val dest = partitioner.destination(vId)
    extraMapping.get(dest) match {
      case Some(x) => if (vId.hashCode() % 2 == 0) dest else x
      case None => dest
    }
  }
}
