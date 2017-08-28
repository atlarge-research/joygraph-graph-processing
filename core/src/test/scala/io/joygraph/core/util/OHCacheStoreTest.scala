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
}
