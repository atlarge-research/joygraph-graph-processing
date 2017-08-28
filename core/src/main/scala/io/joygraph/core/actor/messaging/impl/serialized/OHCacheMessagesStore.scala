package io.joygraph.core.actor.messaging.impl.serialized
import io.joygraph.core.util.DirectByteBufferGrowingOutputStream
import io.joygraph.core.util.collection.ohc.OHCDirectByteBufferStreamMap

import scala.collection.mutable


object OHCacheMessagesStore {
  private[OHCacheMessagesStore] class Partition
  (clazzI : Class[Any],
   partitionIndex : Int,
   totalPartitions : Int,
   maxMessageSize : Int)
    extends PartitionedMessagesStore.Partition(partitionIndex, totalPartitions, maxMessageSize) {
    override def createNextMessages: mutable.Map[Any, DirectByteBufferGrowingOutputStream] = {
      new OHCDirectByteBufferStreamMap[Any](clazzI)
    }

    override def createCurrentMessages: mutable.Map[Any, DirectByteBufferGrowingOutputStream] = {
      new OHCDirectByteBufferStreamMap[Any](clazzI)
    }

    override def postHandleMessage[I](dst: I, os: DirectByteBufferGrowingOutputStream): Unit = {
      nextMessagesRaw.update(dst, os)
    }
  }
}

class OHCacheMessagesStore
(clazzI : Class[Any],
 numPartitions : Int,
 maxMessageSize : Int,
 errorReporter : (Throwable) => Unit) extends PartitionedMessagesStore(numPartitions, maxMessageSize, errorReporter) {
  override def createPartitionInstance(index: WorkerId, numPartitions: WorkerId, maxEdgeSize: WorkerId) = new OHCacheMessagesStore.Partition(clazzI, index, numPartitions, maxEdgeSize)
}
