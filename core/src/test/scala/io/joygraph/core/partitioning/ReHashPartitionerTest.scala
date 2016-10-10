package io.joygraph.core.partitioning

import io.joygraph.core.partitioning.impl.{NaiveHashPartitioner, ReHashPartitioner, ReHashPartitionerV2, VertexHashPartitioner}
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq

class ReHashPartitionerTest extends FunSuite {

  test("ihihihi") {
    val vertices = 100000 to 200000
    var workers = 0 until 10
    var partitioner : VertexPartitioner = new VertexHashPartitioner(workers.size)

    var partitions: Map[Int, IndexedSeq[Int]] = workers.map{ workerId =>
      workerId -> vertices.filter(partitioner.destination(_) == workerId)
    }.toMap

    partitions.toIndexedSeq.sortBy(_._1).foreach{ case (workerId , workerVertices) =>
      println(s"$workerId: ${workerVertices.size}")
    }
    println()

    workers = 0 until 11
    val builder = ReHashPartitionerV2.newBuilder()
    builder.partitioner(partitioner)
//    distributing: 3 -> 10: 0.05063063063063067
//    distributing: 2 -> 10: 0.08603642671292284
//    distributing: 5 -> 10: 0.10619168787107722
//    distributing: 6 -> 10: 0.07398945518453431
    builder.distribute(3, (10, 0.05063063063063067))
    builder.distribute(2, (10, 0.08603642671292284))
    builder.distribute(5, (10, 0.10619168787107722))
    builder.distribute(6, (10, 0.07398945518453431))
    partitioner = builder.build()

    // generate new partitions
    partitions = workers.map{ workerId =>
      workerId -> vertices.filter(partitioner.destination(_) == workerId)
    }.toMap

    //      if (i == generations - 1) {
    partitions.toIndexedSeq.sortBy(_._1).foreach{ case (workerId , workerVertices) =>
      println(s"$workerId: ${workerVertices.size}")
    }
    println()
    //      }

    println("aass")
  }

  test("haha") {
    val vertices = 100000 to 200000
    var workers = 0 until 10
    var partitioner : VertexPartitioner = new VertexHashPartitioner(workers.size)

    var partitions: Map[Int, IndexedSeq[Int]] = workers.map{ workerId =>
      workerId -> vertices.filter(partitioner.destination(_) == workerId)
    }.toMap

    partitions.toIndexedSeq.sortBy(_._1).foreach{ case (workerId , workerVertices) =>
      println(s"$workerId: ${workerVertices.size}")
    }
    println()

    val generations = 4
    for (i <- 0 until generations) {
      val numNewWorkers = 10
      val newWorkerSize = workers.size + numNewWorkers
      val oldWorkerSize = workers.size
      val newWorkerIds = (oldWorkerSize until oldWorkerSize + numNewWorkers).toArray
      workers = 0 until newWorkerSize
      val builder = new ReHashPartitioner.Builder()
      builder.partitioner(partitioner)

      for (j <- 0 until oldWorkerSize) {
        builder.distribute(j, (newWorkerIds, numNewWorkers / newWorkerSize.toDouble))
      }
      partitioner = builder.build()

      // generate new partitions
      partitions = workers.map{ workerId =>
        workerId -> vertices.filter(partitioner.destination(_) == workerId)
      }.toMap

//      if (i == generations - 1) {
        partitions.toIndexedSeq.sortBy(_._1).foreach{ case (workerId , workerVertices) =>
          println(s"$workerId: ${workerVertices.size}")
        }
        println()
//      }
    }

    println("aass")
  }

  test("haha2") {
    val vertices = 100000 to 200000
    var workers = 0 until 10
    var partitioner : VertexPartitioner = new VertexHashPartitioner(workers.size)

    var partitions: Map[Int, IndexedSeq[Int]] = workers.map{ workerId =>
      workerId -> vertices.filter(partitioner.destination(_) == workerId)
    }.toMap

    val generations = 40
    for (i <- 0 until generations) {
      val numNewWorkers = 1
      val newWorkerSize = workers.size + numNewWorkers
      val oldWorkerSize = workers.size
      workers = 0 until newWorkerSize
      partitioner = new NaiveHashPartitioner(partitioner, oldWorkerSize, newWorkerSize)

      // generate new partitions
      partitions = workers.map{ workerId =>
        workerId -> vertices.filter(partitioner.destination(_) == workerId)
      }.toMap

      //      if (i == generations - 1) {
      partitions.toIndexedSeq.sortBy(_._1).foreach{ case (workerId , workerVertices) =>
        println(s"$workerId: ${workerVertices.size}")
//        println(s"$workerId: ${workerVertices}")
      }
      println()
      //      }
    }
  }

}
