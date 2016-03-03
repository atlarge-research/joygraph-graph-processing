package nl.joygraph.core.actor.messaging

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import nl.joygraph.core.partitioning.VertexPartitioner

import scala.collection.concurrent.TrieMap

trait TrieMapSerializedMessageStore[I,M] extends MessageStore[I,M] {

  private[this] val messaging = new TrieMapSerializedMessaging[I,M]
  private[this] val kryos : TrieMap[Int, Kryo] = TrieMap.empty
  private[this] val kryoOutputs : TrieMap[Int, KryoOutput] = TrieMap.empty
  private[this] val reusableIterables : TrieMap[Int, ReusableIterable[M]] = TrieMap.empty
  private[this] var _maxMessageSize : Option[Int] = None
  protected[this] var partitioner : VertexPartitioner
  protected[this] val clazzM : Class[M]

  private[this] def maxMessageSize : Int = _maxMessageSize match {
    case Some(x) => x
    case None => 4096
  }
  def maxMessageSize_=(maxMessageSize : Int) = _maxMessageSize = Some(maxMessageSize)

  private[this] def kryo(index : Int) : Kryo = {
    kryos.getOrElseUpdate(index, new Kryo)
  }

  override protected[this] def _handleMessage(index : Int, dstMPair : (I, M)): Unit = {
    implicit val kryoInstance = kryo(index)
    kryoInstance.synchronized {
      implicit val kryoOutput = kryoOutputs.getOrElseUpdate(index, new KryoOutput(maxMessageSize, maxMessageSize))
      val (dst, m) = dstMPair
      messaging.add(dst, m)
    }
  }

  override protected[this] def messages(dst : I) : Iterable[M] = {
    val index = partitioner.destination(dst)
    implicit val iterable = reusableIterables.getOrElseUpdate(index, {
      val reusableIterable = new ReusableIterable[M]
      reusableIterable.clazz(clazzM)
      reusableIterable.input(new ByteBufferInput(maxMessageSize, maxMessageSize))
      reusableIterable
    })
    iterable.synchronized{
      iterable.kryo(kryo(index))
      messaging.get(dst)
    }
  }
  override protected[this] def messagesOnSuperStepComplete() = {
    messaging.onSuperStepComplete()
  }

  override protected[this] def emptyCurrentMessages : Boolean = {
    messaging.emptyCurrentMessages
  }
}
