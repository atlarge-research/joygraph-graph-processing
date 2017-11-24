package io.joygraph.core.actor.vertices.impl.serialized

import io.joygraph.core.actor.vertices.impl.serialized.OpenHashMapSerializedVerticesStore.Partition
import io.joygraph.core.util._
import io.joygraph.core.util.collection.ohc.OHCSerializedMap

import scala.collection.mutable

object OpenHashMapSerializedVerticesStore {

  protected[OpenHashMapSerializedVerticesStore] class Partition[I, V, E]
  (override protected[this] val clazzI: Class[I],
   override protected[this] val clazzE: Class[E],
   override protected[this] val clazzV: Class[V],
   partitionIndex: Int,
   numPartitions: Int,
   maxEdgeSize: Int)
    extends PartitionedVerticesStore.Partition[I, V, E](clazzI, clazzE, clazzV, partitionIndex, numPartitions, maxEdgeSize) {

    override def createHaltedMap: mutable.Map[I, Boolean] = mutable.OpenHashMap.empty

    override def createEdgesMap: mutable.Map[I, DirectByteBufferGrowingOutputStream] = mutable.OpenHashMap.empty

    override def createValuesMap: mutable.Map[I, V] = mutable.OpenHashMap.empty

  }

}

class OpenHashMapSerializedVerticesStore[I,V,E]
(clazzI : Class[I],
  clazzE : Class[E],
  clazzV : Class[V],
  numPartitions : Int,
  maxEdgeSize : Int,
  errorReporter : (Throwable) => Unit)
  extends PartitionedVerticesStore[I,V,E](clazzI, clazzE, clazzV, numPartitions, maxEdgeSize, errorReporter) {

  override def createPartitionInstance(index: Int, numPartitions: Int, maxEdgeSize: Int): Partition[I, V, E] = {
    new OpenHashMapSerializedVerticesStore.Partition[I,V,E](clazzI, clazzE, clazzV, index, numPartitions, maxEdgeSize)
  }
}

