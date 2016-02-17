package nl.joygraph.programs

import com.typesafe.config.Config
import nl.joygraph.core.program.{Vertex, VertexProgram}

class BFS(val superStep : Int) extends VertexProgram[Long,Int,Unit,Int] {

  var sourceId : Long = -1
  override def load(conf : Config): Unit = {
    sourceId = conf.getLong("source_id")
  }

  override def run(v: Vertex[Long, Int, Unit, Int], superStep : Int): Boolean = {
    if (superStep == 0) {
      if (v.id == sourceId) {
        v.value = superStep
        v.sendAll(superStep)
        true
      } else {
        v.value = -1
        false
      }
    } else {
      if (v.value < 0) {
        v.value = superStep
        v.sendAll(superStep)
      }
      true
    }
  }
}
