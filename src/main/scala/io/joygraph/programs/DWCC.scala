package io.joygraph.programs

import com.typesafe.config.Config
import io.joygraph.core.program.{NullClass, Vertex, VertexProgram}

import scala.collection.mutable

class DWCC extends VertexProgram[Long, Long, NullClass, Long] {
  /**
    * Load parameters from vertex program
    *
    * @param conf
    */
  override def load(conf: Config): Unit = {}//noop

  private[this] val edgeSet = new mutable.HashSet[Long]()

  /**
    * @param v
    * @return true if halting, false if not halting
    */
  override def run(v: Vertex[Long, Long, NullClass], messages: Iterable[Long], superStep: Int): Boolean = {
    // Weakly connected components algorithm treats a directed graph as undirected, so we create the missing edges
    if (superStep == 0) {
      sendAll(v, v.id)
      false
    }
    else if (superStep == 1) {
      edgeSet.clear()
      v.edges.foreach(existingEdge => edgeSet += existingEdge.dst)
      messages.foreach(m => if(!edgeSet.contains(m)) v.addEdge(m, NullClass.SINGLETON))
      val minOtherId = if (v.edges.isEmpty) v.id else v.edges.minBy[Long](_.dst).dst
      v.value = math.min(v.id, minOtherId)
      if (minOtherId < v.id) {
        sendAll(v, minOtherId)
      }
      true
    }
    else {
      val currentComponent: Long = v.value
      val candidateComponent = messages.min
      if (candidateComponent < currentComponent) {
        v.value = candidateComponent
        sendAll(v, candidateComponent)
      }
      true
    }
  }
}
