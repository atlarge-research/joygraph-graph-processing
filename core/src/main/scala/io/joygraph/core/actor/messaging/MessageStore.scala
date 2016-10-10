package io.joygraph.core.actor.messaging

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferInputStream
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.concurrency.Types
import io.joygraph.core.util.serde.{AsyncDeserializer, AsyncSerializer}

import scala.concurrent.Future

trait MessageStore extends Types {

  def importNextMessagesData[I, M]
  (index : ThreadId,
   is : ObjectByteBufferInputStream,
   messagesDeserializer : AsyncDeserializer,
   deserializer : (Kryo, Input) => Message[I],
   clazzI : Class[I],
   currentOutgoingMessageClass : Class[M]
  ): Unit = {
    is.msgType match {
      case 4 => // halted
        messagesDeserializer.deserialize(is, index, deserializer){ implicit dstMPairs =>
          dstMPairs.foreach(_handleMessage(index, _, clazzI, currentOutgoingMessageClass))
        }
      case _ => //noop
    }
  }

  def exportMessages[I,M](vId : I, clazzM : Class[M], threadId : ThreadId, workerId : WorkerId, asyncSerializer: AsyncSerializer, outputHandler : ByteBuffer => Future[ByteBuffer]) = {
    val vMessages = nextMessages(vId, clazzM)
    if (vMessages.nonEmpty) {
      vMessages.foreach{ m =>
        asyncSerializer.serialize[M](threadId, workerId, m, (kryo, output, o) => {
          kryo.writeObject(output, vId)
          kryo.writeObject(output, o)
        })(outputHandler)
      }

      // release messages
      releaseMessages(vMessages, clazzM)
    }
  }

  def addAllNextMessages(elasticMessagesStore: MessageStore) : Unit = ???

  def _handleMessage[I](index: WorkerId, message: Message[I], clazzI: Class[I], clazzM: Class[_])
  def nextMessages[I,M](dst : I, clazzM : Class[M]) : Iterable[M]
  def messages[I,M](dst : I, clazzM : Class[M]) : Iterable[M]
  protected[messaging] def removeMessages[I](dst : I)
  protected[messaging] def removeNextMessages[I](dst : I)
  def removeNextMessages(workerId: WorkerId, partitioner: VertexPartitioner) : Unit = ???
  def releaseMessages(messages : Iterable[_ <: Any], clazz : Class[_ <: Any])
  def messagesOnBarrier()
  def emptyNextMessages : Boolean

  // TODO move to a different interface
  /**
    * Pooling for serialized message iterables
    */
  def setReusableIterableFactory(factory: => ReusableIterable[Any]): Unit = {}
}
