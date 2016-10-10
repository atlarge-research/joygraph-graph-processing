package io.joygraph.core.partitioning.impl

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.Config
import io.joygraph.core.partitioning.VertexPartitioner

import scala.collection.mutable

object ReHashPartitionerV2 {

  def newBuilder() : Builder = {
    new Builder()
  }

  class Builder {
    private[this] val extraMapping = mutable.OpenHashMap.empty[Int, (Int, Int, Int)]
    private[this] var p : VertexPartitioner = _

    def distribute(from : Int, to : (Int, Double)) : Builder = {
      val (workerId, percentage) = to
      val parts = math.max(Math.ceil(1.0 / percentage).toInt, 1)
//      extraMapping(from) = (workerId, parts, Random.nextInt(parts))
      extraMapping(from) = (workerId, parts, 0)
      println(from -> extraMapping(from))
      this
    }

    def partitioner(p : VertexPartitioner) : Builder = {
      this.p = p
      this
    }

    def build(): ReHashPartitionerV2 = {
      val mapAsArray = new Array[(Int, Int, Int)](extraMapping.keys.max + 1)

      extraMapping.foreach{
        case (workerId, value) => mapAsArray(workerId) = value
      }

      new ReHashPartitionerV2(p, mapAsArray)
    }
  }
}

class ReHashPartitionerV2 protected(partitioner : VertexPartitioner, extraMapping : Array[(Int, Int, Int)]) extends VertexPartitioner {
  private[this] val cache = new ConcurrentHashMap[Any, Int]()
  partitioner match {
    case partitioner1: ReHashPartitionerV2 =>
      partitioner1.clearCache()
    case _ =>
  }

  def clearCache(): Unit = {
    cache.clear()
  }

  override def init(conf: Config): Unit = {
    //
  }

  def computeDest(vId : Any): Int = {
    val dest = partitioner.destination(vId)
    if (dest >= extraMapping.length) {
      dest
    } else {
      Option(extraMapping(dest)) match {
        case Some((destination, parts, random)) =>
          val targetHashCode = vId.hashCode()
          if ((targetHashCode % parts) == random) {
            destination
          } else {
            dest
          }
        case None => dest
      }
    }
  }

  override def destination(vId: Any): Int = {
    cache.computeIfAbsent(vId, computeDest)
  }
}
