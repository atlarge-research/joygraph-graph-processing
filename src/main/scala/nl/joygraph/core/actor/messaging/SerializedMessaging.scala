package nl.joygraph.core.actor.messaging

import com.esotericsoftware.kryo.Kryo

trait SerializedMessaging[I,M]  {
  protected[this] val EMPTY_MESSAGES = Iterable.empty[M]

  def onSuperStepComplete()

  /**
    * Add message to source
    */
  def add(source: I, message : M)(implicit kryo: Kryo, output : KryoOutput)

  /**
    * Retrieve messages for source if any
    */
  def get(source : I)(implicit reusableIterable : ReusableIterable[M]) : Iterable[M]

  def emptyCurrentMessages : Boolean
}
