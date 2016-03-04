package nl.joygraph.programs

import com.typesafe.config.Config
import nl.joygraph.core.program.{Edge, NullClass, Vertex, VertexProgram}

class ConnectedComponents extends VertexProgram[Long, Long, NullClass, Long] {
  /**
    * Load parameters from vertex program
    *
    * @param conf
    */
  override def load(conf: Config): Unit = {}//noop

  /**
    * @param v
    * @return true if halting, false if not halting
    */
  override def run(v: Vertex[Long, Long, NullClass, Long], messages: Iterable[Long], superStep: Int): Boolean = {
    var currentComponent: Long = v.value

    // Weakly connected components algorithm treats a directed graph as undirected, so we create the missing edges
    if (superStep == 0) {
      v.value = v.id
      v.sendAll(v.id)
      return false
    }
    else if (superStep == 1) {
      messages.foreach(m => v.addEdge(m, NullClass.SINGLETON))
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
