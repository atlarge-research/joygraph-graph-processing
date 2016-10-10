package io.joygraph.core.partitioning.impl

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.Config
import io.joygraph.core.partitioning.VertexPartitioner

import scala.collection.mutable
import scala.util.Random

object ReHashPartitioner {
  class Builder {
    private[this] val extraMapping = mutable.OpenHashMap.empty[Int, (Array[Int], Int, Int)]
    private[this] var p : VertexPartitioner = _

    private[this] def generateRandomPlacementArray(workerIds : Array[Int]): Array[Int] = {
      val arraySize = 10000
      val newWorkerIds = new Array[Int](arraySize)
      var i = 0
      while (i < newWorkerIds.length) {
        val index = Random.nextInt(workerIds.length)
        newWorkerIds(i) = workerIds(index)
        i += 1
      }
      newWorkerIds
    }

    def distribute(from : Int, to : (Array[Int], Double)) : Builder = {
      val (workerIds, percentage) = to
      val parts = math.max(Math.ceil(1.0 / percentage).toInt, 1)
//      val genWorkerIds = generateRandomPlacementArray(workerIds)
      extraMapping(from) = (workerIds, parts, Random.nextInt(parts))
//      extraMapping(from) = (genWorkerIds, parts, Random.nextInt(parts))
//      extraMapping(from) = (workerIds, parts, 0)
      this
    }
    def partitioner(p : VertexPartitioner) : Builder = {
      this.p = p
      this
    }
    def build(): ReHashPartitioner = {
      val mapAsArray = new Array[(Array[Int], Int, Int)](extraMapping.keys.max + 1)

      extraMapping.foreach{
        case (workerId, value) => mapAsArray(workerId) = value
      }

      new ReHashPartitioner(p, mapAsArray)
    }
  }
}

class ReHashPartitioner protected(partitioner : VertexPartitioner, extraMapping : Array[(Array[Int], Int, Int)]) extends VertexPartitioner {

  private[this] val cache = new ConcurrentHashMap[Any, Int]()
  partitioner match {
    case partitioner1: ReHashPartitioner =>
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
        case Some((destinations, parts, random)) =>
          val targetHashCode = vId.hashCode()
          if ((targetHashCode % parts) == random) {
            // do something depending on the hashcode
            val finalDest = destinations(targetHashCode % destinations.length)
            finalDest
          } else {
            dest
          }
        case None => dest
      }
    }
  }

  override def destination(vId: Any): Int = {
    cache.computeIfAbsent(vId, computeDest)
//    computeDest(vId)
  }
}
