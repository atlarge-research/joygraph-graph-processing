package io.joygraph.core.actor.messaging

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable

trait SerializedMessaging  {
  protected[this] val EMPTY_MESSAGES : Iterable[Any] = Iterable.empty[Any]

  def onSuperStepComplete()

  /**
    * Add message to source
    */
  def add[I,M](source: I, message : M)(implicit kryo: Kryo, output : KryoOutput)

  /**
    * Retrieve messages for source if any
    */
  def get[I,M](source : I)(implicit reusableIterable : ReusableIterable[M]) : Iterable[M]

  def emptyCurrentMessages : Boolean
}
