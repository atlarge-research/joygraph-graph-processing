package io.joygraph.programs

import com.typesafe.config.Config
import io.joygraph.core.program.{NullClass, Vertex, VertexProgram}

object BFS {
  val UNVISITED : Long = Long.MaxValue
}

class BFS extends VertexProgram[Long, Long, NullClass, Long] {
  var sourceId : Long = -1
  override def load(conf : Config): Unit = {
    sourceId = conf.getLong("source_id")
  }

  override def run(v: Vertex[Long, Long, NullClass, Long], messages : Iterable[Long], superStep : Int): Boolean = {
    if (superStep == 0) {
      if (v.id == sourceId) {
        v.value = superStep
        v.sendAll(superStep)
      } else {
        v.value = BFS.UNVISITED
      }
    } else {
      if (v.value == BFS.UNVISITED) {
        v.value = superStep
        v.sendAll(superStep)
      }
    }
    true
  }

}
