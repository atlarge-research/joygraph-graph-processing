package io.joygraph.programs

import com.typesafe.config.Config
import io.joygraph.core.program.{Edge, NullClass, Vertex, VertexProgram}
import it.unimi.dsi.fastutil.longs.LongOpenHashSet

import scala.collection.JavaConversions._

// TODO WIP
class DirectedWeaklyConnectedComponentsComputation extends VertexProgram[Long, Long, NullClass, Long] {
  /**
    * Load parameters from vertex program
    *
    * @param conf
    */
  override def load(conf: Config): Unit = {}//noop

  private val edgeSet = new LongOpenHashSet()

  /**
    * @param v
    * @return true if halting, false if not halting
    */
  override def run(v: Vertex[Long, Long, NullClass, Long], messages: Iterable[Long], superStep: Int): Boolean = {
    var currentComponent: Long = v.value

    // Weakly connected components algorithm treats a directed graph as undirected, so we create the missing edges
    if (superStep == 0) {
      v.sendAll(v.id)
      return false
    }
    else if (superStep == 1) {
      v.edges.foreach(existingEdge => edgeSet += existingEdge.dst)
      messages.foreach(m => if(!edgeSet.contains(m)) v.addEdge(m, NullClass.SINGLETON))
      
      return false
    }
    else if (superStep == 2) {
      v.edges.foreach{
        case Edge(neighbor, _) =>
          if (neighbor < currentComponent) {
            currentComponent = neighbor
          }
      }
      if (currentComponent != v.value) {
        v.value = currentComponent
        v.edges.foreach {
          case Edge(neighbor, _) =>
            if (neighbor > currentComponent) {
              v.send(neighbor, v.value)
            }
        }
      }
      return true
    }

    val candidateComponent = messages.max
    // propagate new component id to the neighbors
    if (candidateComponent < currentComponent) {
      v.value = candidateComponent
      v.sendAll(candidateComponent)
    }
    true
  }
}
