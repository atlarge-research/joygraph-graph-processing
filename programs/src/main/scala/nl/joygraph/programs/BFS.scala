package nl.joygraph.programs

import com.typesafe.config.Config
import nl.joygraph.core.program.{NullClass, Vertex, VertexProgram}

class BFS extends VertexProgram[Long,Int, NullClass, Int] {

  var sourceId : Long = -1
  override def load(conf : Config): Unit = {
    sourceId = conf.getLong("source_id")
  }

  override def run(v: Vertex[Long, Int, NullClass, Int], messages : Iterable[Int], superStep : Int): Boolean = {
    if (superStep == 0) {
      if (v.id == sourceId) {
        v.value = superStep
        v.sendAll(superStep)
      } else {
        v.value = -1
      }
    } else {
      if (v.value < 0) {
        v.value = superStep
        v.sendAll(superStep)
      }
    }
    true
  }
}
