package io.joygraph.core.actor.messaging

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import io.joygraph.core.util.SimplePool
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferInputStream
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.serde.{AsyncDeserializer, AsyncSerializer}

import scala.concurrent.{ExecutionContext, Future}

trait MessageStore {

  protected[this] def importCurrentMessagesData[I, M]
  (index : Int,
   is : ObjectByteBufferInputStream,
   messagesDeserializer : AsyncDeserializer,
   deserializer : (Kryo, Input) => (I, Any),
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

  protected[this] def exportAndRemoveMessages[I,M](vId : I, clazzM : Class[M], index : Int, asyncSerializer: AsyncSerializer, outputHandler : ByteBuffer => Future[ByteBuffer])(implicit exeContext : ExecutionContext) = {
    val vMessages = nextMessages(vId, clazzM)
    if (vMessages.nonEmpty) {
      vMessages.foreach{ m =>
        asyncSerializer.serialize[M](index, m, (kryo, output, o) => {
          kryo.writeObject(output, vId)
          kryo.writeObject(output, o)
        })(outputHandler)
      }

      // release messages
      releaseMessages(vMessages, clazzM)

      // remove messages
      removeMessages(vId)
    }
  }

  protected[this] def _handleMessage[I](index : Int, dstMPair : (I, _ <: Any), clazzI : Class[I], clazzM: Class[_ <: Any])
  protected[this] def nextMessages[I,M](dst : I, clazzM : Class[M]) : Iterable[M]
  protected[this] def messages[I,M](dst : I, clazzM : Class[M]) : Iterable[M]
  protected[this] def removeMessages[I](dst : I)
  protected[this] def releaseMessages(messages : Iterable[_ <: Any], clazz : Class[_ <: Any])
  protected[this] def messagesOnBarrier()
  protected[this] def emptyNextMessages : Boolean

  // TODO move to a different interface
  /**
    * Pooling for serialized message iterables
    */
  protected[this] def setReusableIterablePool(pool : SimplePool[ReusableIterable[Any]]) : Unit = {}
}
