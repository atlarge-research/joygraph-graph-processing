package io.joygraph.programs

import com.typesafe.config.Config
import io.joygraph.core.program.{Edge, NewVertexProgram, SuperStepFunction, Vertex}
object SSSP {
  val SOURCE_ID_CONF_KEY = "source_id"
}
class SSSP extends NewVertexProgram[Long, Double, Double]{

  private[this] var sourceId : Long = _

  override def load(conf: Config): Unit = sourceId = conf.getLong(SSSP.SOURCE_ID_CONF_KEY)

  override def run(): PartialFunction[Int, SuperStepFunction[Long, Double, Double, _, _]] = {
    case superStep @ _ => new SuperStepFunction(this, classOf[Double], classOf[Double]) {
      override def func: (Vertex[Long, Double, Double], Iterable[Double]) => Boolean = (v, m) => {
        superStep match {
          case 0 =>
            if (v.id == sourceId) {
              v.value = 0.0
              informNeighbours(v)
            } else {
              v.value = Double.PositiveInfinity
            }
          case _ =>
            val minDist = m.min
            if (minDist < v.value) {
              v.value = minDist
              informNeighbours(v)
            }
        }
        true
      }
      private[this] def informNeighbours(v : Vertex[Long, Double, Double]): Unit = {
        v.edges.foreach{
          case Edge(id, value) =>
            send(v.value + value, id)
        }
      }
    }
  }
}
