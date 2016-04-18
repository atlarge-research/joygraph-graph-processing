package io.joygraph.core.util.serde

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import io.joygraph.core.actor.vertices.VertexEdge
import io.joygraph.core.util.TypeUtil

/**
  * Not sharable
  */
class EdgeDeserializer[I,E](clazzI : Class[I], clazzE : Class[E]) {

  private[this] val voidOrUnitClass = TypeUtil.unitOrVoid(clazzE)
  private[this] val vertexEdge = VertexEdge[I,E](null.asInstanceOf[I], null.asInstanceOf[I], null.asInstanceOf[E])

  def edgeDeserializer(kryo : Kryo, input : Input) : VertexEdge[I,E] = {
    vertexEdge.src = kryo.readObject(input, clazzI)
    vertexEdge.dst = kryo.readObject(input, clazzI)
    if (!voidOrUnitClass) {
      vertexEdge.value = kryo.readObject(input, clazzE)
    }
    vertexEdge
  }
}
