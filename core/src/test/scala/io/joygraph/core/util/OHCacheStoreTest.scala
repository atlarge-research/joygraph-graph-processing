package io.joygraph.core.util

import io.joygraph.core.actor.vertices.impl.serialized.OHCacheSerializedVerticesStore
import io.joygraph.core.program.Edge
import org.scalatest.FunSuite

import scala.collection.mutable

class OHCacheStoreTest extends FunSuite {
  test("Test store") {
    val verticesStore: OHCacheSerializedVerticesStore[Long, Int, Long] = new OHCacheSerializedVerticesStore[Long, Int, Long](
      classOf[Long],
      classOf[Long],
      classOf[Int], 1, 4096, (wat) => println(wat))
    verticesStore.addEdge(1L, 2L, 1L)
    verticesStore.addEdge(1L, 3L, 4L)
    verticesStore.addEdge(1L, 4L, 5L)
    verticesStore.addEdge(1L, 5L, 6L)
    val edges = verticesStore.edges(1L)

    val simpleTest = mutable.Map.empty[Long, Long]
    simpleTest += 2L -> 1L
    simpleTest += 3L -> 4L
    simpleTest += 4L -> 5L
    simpleTest += 5L -> 6L

    assert(edges.nonEmpty)

    assert(edges.size == 4)

    edges.foreach{
      case (Edge(dst, value)) =>
        assert(simpleTest(dst) == value)
    }

  }

  // Add optimistic test
  test ("Bigger store") {
    val verticesStore: OHCacheSerializedVerticesStore[Long, Int, Long] = new OHCacheSerializedVerticesStore[Long, Int, Long](
      classOf[Long],
      classOf[Long],
      classOf[Int], 1, 4096, (wat) => println(wat))

    val numEntries = 900
    val numEdges = 50
    for (i <- 1 until numEntries) {
      for (j <- 1 until numEdges) {
        verticesStore.addEdge(i, i+j, i+j+1)
      }
      for (i <- 1 until i + 1) {
        val edges = verticesStore.edges(i)
        val edgesAsMap = edges.map{
          case Edge(a, b) =>
            a -> b
        }.toMap
        try {
          for (j <- 1 until numEdges) {
            assert(edgesAsMap(i + j) == i + j + 1)
          }
        } catch {
          case e: Exception =>
            println(edgesAsMap)
            throw e
        }

      }
    }
  }
}
