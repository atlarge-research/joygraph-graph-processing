package io.joygraph.core.partitioning.impl

import com.typesafe.config.Config
import io.joygraph.core.partitioning.VertexPartitioner

import scala.collection.mutable

object ReHashPartitioner {
  class Builder {
    private[this] val extraMapping = mutable.OpenHashMap.empty[Int, (Array[Int], Double)]
    private[this] var p : VertexPartitioner = _

    def distribute(from : Int, to : (Array[Int], Double)) : Builder = {
      extraMapping(from) = to
      this
    }
    def partitioner(p : VertexPartitioner) : Builder = {
      this.p = p
      this
    }
    def build(): ReHashPartitioner = {
      new ReHashPartitioner(p, extraMapping.toMap)
    }
  }
}

class ReHashPartitioner protected(partitioner : VertexPartitioner, extraMapping : Map[Int, (Array[Int], Double)]) extends VertexPartitioner {

  override def init(conf: Config): Unit = {
    //
  }

  override def destination(vId: Any): Int = {
    val dest = partitioner.destination(vId)
    extraMapping.get(dest) match {
      case Some((destinations, percentage)) =>
        val parts = math.max((1.0 / percentage).toInt, 1)
        val dstWorker = vId.hashCode() % parts
        if (dstWorker == 0)
          destinations(vId.hashCode() % destinations.length)
        else
          dest
      case None => dest
    }
  }
}
