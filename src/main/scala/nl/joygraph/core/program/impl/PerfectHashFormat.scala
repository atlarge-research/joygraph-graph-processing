package nl.joygraph.core.program.impl

import nl.joygraph.core.program.{Edge, NullClass, Vertex, VertexProgramLike}

import scala.collection.mutable.ArrayBuffer

class PerfectHashValue {
  var id : Long = _
  var edges : ArrayBuffer[Long] = ArrayBuffer.empty
}
case class PerfectHashMessage[I](srcOrg : I, srcNew : Long) {
}

case class Mapper[I](newId : Option[Long], oldToNew : Option[Map[I, Long]])

class PerfectHashFormat[I] extends VertexProgramLike[I, PerfectHashValue, NullClass, PerfectHashMessage[I]] {

  def run(v: Vertex[I, PerfectHashValue, NullClass, PerfectHashMessage[I]], superStep: Int, magic : Mapper[I]): Boolean = {
    if (superStep == 0) {
      magic.newId.foreach{ x =>
        v.value.id = x
        v.sendAll(PerfectHashMessage[I](v.id, x))
      }
    } else {
      magic.oldToNew.foreach {
        mapping =>
          v.value.edges ++= v.edges.flatMap{case Edge(dst, _ ) => mapping.get(dst)}
      }
    }

    true
  }
}
