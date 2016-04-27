package io.joygraph.core.util

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.util.buffers.KryoOutput

import scala.collection.concurrent.TrieMap

trait KryoSerialization {

  private[this] val kryos : TrieMap[Int, Kryo] = TrieMap.empty
  private[this] val kryoOutputs : TrieMap[Int, KryoOutput] = TrieMap.empty

  protected[this] def kryoOutputFactory : KryoOutput

  protected[this] def kryoOutput(index : Int) : KryoOutput = {
    kryoOutputs.getOrElseUpdate(index, kryoOutputFactory)
  }

  protected[this] def kryo(index : Int) : Kryo = {
    // TODO use kryoFactory or some factory method
    kryos.getOrElseUpdate(index, new Kryo)
  }

}
