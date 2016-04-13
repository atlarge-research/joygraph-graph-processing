package io.joygraph.core.actor

import java.util.concurrent.atomic.AtomicBoolean

import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.program.{Edge, SuperStepFunction, Vertex}
import io.joygraph.core.util.SimplePool

class VertexComputation[I,V,E]
(currentIncomingMessageClass : Class[_],
 messageStore : MessageStore,
 superStepFunctionPool : SimplePool[SuperStepFunction[I,V,E,_,_]]) {

  private[this] val hasAllHalted = new AtomicBoolean(true)

  def computeVertex(vId : I, verticesStore: VerticesStore[I,V,E], simpleVertexInstancePool : SimplePool[Vertex[I,V,E]]) : Unit = {
    val vMessages = messageStore.messages(vId, currentIncomingMessageClass)
    val vHalted = verticesStore.halted(vId)
    val hasMessage = vMessages.nonEmpty
    if (!vHalted || hasMessage) {
      val value : V = verticesStore.vertexValue(vId)
      val edgesIterable : Iterable[Edge[I,E]] = verticesStore.edges(vId)
      val mutableEdgesIterable : Iterable[Edge[I,E]] = verticesStore.mutableEdges(vId)
      // get vertex impl
      val v : Vertex[I,V,E] = simpleVertexInstancePool.borrow()
      v.load(vId, value, edgesIterable, mutableEdgesIterable)

      // get superstepfunction instance
      val superStepFunctionInstance = superStepFunctionPool.borrow()
      val hasHalted = superStepFunctionInstance(v, vMessages)
      // release superstepfunction instance
      superStepFunctionPool.release(superStepFunctionInstance)
      verticesStore.setVertexValue(vId, v.value)
      verticesStore.setHalted(vId, hasHalted)
      simpleVertexInstancePool.release(v)
      // release vertex impl
      if (!hasHalted) {
        hasAllHalted.set(false)
      }

      verticesStore.releaseEdgesIterable(edgesIterable)
      verticesStore.releaseEdgesIterable(mutableEdgesIterable)
    }
    messageStore.releaseMessages(vMessages, currentIncomingMessageClass)
  }

  def hasHalted : Boolean = hasAllHalted.get()
}
