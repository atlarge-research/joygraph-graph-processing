package io.joygraph.core.util

import io.joygraph.core.actor.vertices.VertexEdge

object ParseUtil {
  def edgeListLineLongLong(l : String, vertexEdge : VertexEdge[Long, Unit]) : Unit = {
    val s = l.split("\\s")
    vertexEdge.src = s(0).toLong
    vertexEdge.dst = s(1).toLong
  }

  def edgeListLineLongLongDouble(l : String, vertexEdge : VertexEdge[Long, Double]) : Unit = {
    val s = l.split("\\s")
    vertexEdge.src = s(0).toLong
    vertexEdge.dst = s(1).toLong
    vertexEdge.value = s(2).toDouble
  }

  def edgeListLineLongLongBooleanWithDefault(l : String, vertexEdge : VertexEdge[Long, Boolean], defaultValue : Boolean) = {
    val s = l.split("\\s")
    vertexEdge.src = s(0).toLong
    vertexEdge.dst = s(1).toLong
    vertexEdge.value = defaultValue
  }
}
