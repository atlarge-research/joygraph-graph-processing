package io.joygraph.programs

import com.typesafe.config.Config
import io.joygraph.core.program._

class BFSCombinable extends NewVertexProgram[Long, Long, Unit] {
  var sourceId : Long = -1
  override def load(conf : Config): Unit = {
    sourceId = conf.getLong(BFS.SOURCE_ID_CONF_KEY)
  }

  override def run(): PartialFunction[Int, SuperStepFunction[Long, Long, Unit,_, _]] = {
    case superStep @ 0 =>
      new SuperStepFunction(this, classOf[Long], classOf[Long]) {
        override def func: (Vertex[Long, Long, Unit], Iterable[Long]) => Boolean =
          (v, m) => {
            if (v.id == sourceId) {
              sendAll(v, superStep)
            } else {
              v.value = BFS.UNVISITED
            }
            true
          }
      }
    case superStep @ _ =>
      new SuperStepFunction(this, classOf[Long], classOf[Long]) with Combinable[Long] {
        override def func: (Vertex[Long, Long, Unit], Iterable[Long]) => Boolean =
          (v, m) => {
            if (v.value == BFS.UNVISITED) {
              v.value = superStep
              sendAll(v, superStep)
            }
            true
          }

        override def combine(m1: Long, m2: Long): Long = math.min(m1, m2)
      }
  }
}
