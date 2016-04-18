package io.joygraph.core.util.serde

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import io.joygraph.core.actor.vertices.VertexEdge
import io.joygraph.core.util.TypeUtil

/**
  * Sharable
  */
class EdgeSerializer[I,E](clazzI : Class[I], clazzE : Class[E]) {

  private[this] val voidOrUnitClass = TypeUtil.unitOrVoid(clazzE)

  def edgeSerializer(kryo : Kryo, output : Output, o : VertexEdge[I,E]) : Unit = {
    if (voidOrUnitClass) {
      kryo.writeObject(output, o.src)
      kryo.writeObject(output, o.dst)
    } else {
      kryo.writeObject(output, o.src)
      kryo.writeObject(output, o.dst)
      kryo.writeObject(output, o.value)
    }
  }
}
