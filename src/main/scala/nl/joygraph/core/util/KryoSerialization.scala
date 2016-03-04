package nl.joygraph.core.util

import com.esotericsoftware.kryo.Kryo
import nl.joygraph.core.actor.messaging.KryoOutput

import scala.collection.concurrent.TrieMap

trait KryoSerialization {

  private[this] val kryos : TrieMap[Int, Kryo] = TrieMap.empty
  private[this] val kryoOutputs : TrieMap[Int, KryoOutput] = TrieMap.empty
  private[this] var _maxMessageSize : Option[Int] = None

  protected[this] def maxMessageSize : Int = _maxMessageSize match {
    case Some(x) => x
    case None => 4096
  }

  protected[this] def maxMessageSize_=(maxMessageSize : Int) = _maxMessageSize = Some(maxMessageSize)

  protected[this] def kryoOutput(index : Int) : KryoOutput = {
    kryoOutputs.getOrElseUpdate(index, new KryoOutput(maxMessageSize, maxMessageSize))
  }

  protected[this] def kryo(index : Int) : Kryo = {
    kryos.getOrElseUpdate(index, new Kryo)
  }

}
