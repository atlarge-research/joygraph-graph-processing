package io.joygraph.core.actor.vertices.impl.serialized

import io.joygraph.core.actor.vertices.impl.serialized
import io.joygraph.core.util.DirectByteBufferGrowingOutputStream
import io.joygraph.core.util.collection.ohc.{OHCBooleanValueMap, OHCDirectByteBufferStreamMap}

import scala.collection.mutable

object OHCacheSerializedVerticesStore {
  class Partition[I,V,E]
  (clazzI : Class[I],
   clazzE : Class[E],
   clazzV : Class[V],
   partitionIndex : Int,
   numPartitions : Int,
   maxEdgeSize : Int,)
    extends PartitionedVerticesStore.Partition[I,V,E](clazzI, clazzE, clazzV, partitionIndex, numPartitions, maxEdgeSize) {
    override def createHaltedMap: mutable.Map[I, Boolean] = new OHCBooleanValueMap[I](clazzI)

    override def createEdgesMap: mutable.Map[I, DirectByteBufferGrowingOutputStream] = new OHCDirectByteBufferStreamMap[I](clazzI)

    override def createValuesMap: mutable.Map[I, V] = ??? // TODO
  }
}

class OHCacheSerializedVerticesStore[I,V,E]
(clazzI : Class[I],
 clazzE : Class[E],
 clazzV : Class[V],
 numPartitions : Int,
 maxEdgeSize : Int,
 errorReporter : (Throwable) => Unit)
  extends PartitionedVerticesStore[I,V,E](clazzI, clazzE, clazzV, numPartitions, maxEdgeSize, errorReporter) {
  override def createPartitionInstance(index: Int, numPartitions: Int, maxEdgeSize: Int) = {
    new OHCacheSerializedVerticesStore.Partition[I,V,E](clazzI, clazzE, clazzV, index, numPartitions, maxEdgeSize)
  }
}
