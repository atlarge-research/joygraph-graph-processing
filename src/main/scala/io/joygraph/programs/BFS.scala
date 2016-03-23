package io.joygraph.programs

import com.typesafe.config.Config
import io.joygraph.core.program.{HomogeneousVertexProgram, NullClass, Vertex}

object BFS {
  val UNVISITED : Long = Long.MaxValue
  val SOURCE_ID_CONF_KEY = "source_id"
}

class BFS extends HomogeneousVertexProgram[Long, Long, NullClass, Long] {
  var sourceId : Long = -1
  override def load(conf : Config): Unit = {
    sourceId = conf.getLong(BFS.SOURCE_ID_CONF_KEY)
  }

  override def run(v: Vertex[Long, Long, NullClass], messages: Iterable[Long], superStep: Int)(implicit send: (Long, Long) => Unit, sendAll: (Long) => Unit): Boolean = {
    if (superStep == 0) {
      if (v.id == sourceId) {
        v.value = superStep
        sendAll(v.value)
      } else {
        v.value = BFS.UNVISITED
      }
    } else {
      if (v.value == BFS.UNVISITED) {
        v.value = superStep
        sendAll(v.value)
      }
    }
    true
  }
}
