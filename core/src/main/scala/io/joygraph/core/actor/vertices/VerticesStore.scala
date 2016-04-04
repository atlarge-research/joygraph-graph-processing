package io.joygraph.core.actor.vertices

import java.nio.ByteBuffer

import io.joygraph.core.program.{Edge, NullClass}
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferInputStream
import io.joygraph.core.util.concurrency.Types
import io.joygraph.core.util.serde.{AsyncDeserializer, AsyncSerializer}

import scala.collection.parallel.ParIterable
import scala.concurrent.{ExecutionContext, Future}


trait VerticesStore[I,V,E] extends Types {

  protected[this] val clazzI : Class[I]
  protected[this] val clazzE : Class[E]
  protected[this] val clazzV : Class[V]
  protected[this] val isNullClass = clazzE == classOf[NullClass]

  def importVerticesStoreData
  (index : Int,
   is : ObjectByteBufferInputStream,
   haltedDeserializer : AsyncDeserializer,
   idDeserializer : AsyncDeserializer,
   valueDeserializer : AsyncDeserializer,
   edgeDeserializer : AsyncDeserializer
  ) = {
    is.msgType match {
      case 0 => // halted
        haltedDeserializer.deserialize(is, index, (kryo, input) => {
          kryo.readObject(input, clazzI)
        }){ implicit haltedPairs =>
          haltedPairs.foreach(setHalted(_, true))
        }
      case 1 => // id
        idDeserializer.deserialize(is, index, (kryo, input) => {
          kryo.readObject(input, clazzI)
        }){ implicit ids =>
          ids.foreach(addVertex)
        }
      case 2 => // value
        valueDeserializer.deserialize(is, index, (kryo, input) => {
          (kryo.readObject(input, clazzI), kryo.readObject(input, clazzV))
        }){ implicit valuePairs =>
          valuePairs.foreach(x => setVertexValue(x._1, x._2))
        }
      case 3 => // edge
        edgeDeserializer.deserialize(is, index, (kryo, input) => {
          if (!isNullClass) {
            (kryo.readObject(input, clazzI),
              kryo.readObject(input, clazzI),
              kryo.readObject(input, clazzE)
              )
          } else {
            (kryo.readObject(input, clazzI),
              kryo.readObject(input, clazzI),
              null.asInstanceOf[E]
              )
          }
        }){ implicit edgePairs =>
          edgePairs.foreach(x => addEdge(x._1, x._2, x._3))
        }
      case _ => // noop
    }
  }

  /**
    * Note that if halted is false is the default state,
    * only propagate the halted == true
    * @param vId
    */
  def exportHaltedState
  (vId : I, index : ThreadId, workerId : WorkerId, asyncSerializer: AsyncSerializer, outputHandler : ByteBuffer => Future[ByteBuffer])(implicit exeContext : ExecutionContext) = {
    val vHalted = halted(vId)
    if (vHalted) {
      // value of halted is implicit
      asyncSerializer.serialize[I](index, workerId, vId, (kryo, output, o) => {
        kryo.writeObject(output, o)
      })(outputHandler)
    }
  }

  def exportValue(vId : I, index : ThreadId, workerId : WorkerId, asyncSerializer: AsyncSerializer, outputHandler : ByteBuffer => Future[ByteBuffer])(implicit exeContext : ExecutionContext)  = {
    val vValue : V = vertexValue(vId)
    Option(vValue) match {
      case Some(vValue) =>
        asyncSerializer.serialize[(I, V)](index, workerId, (vId, vValue), (kryo, output, o) => {
          kryo.writeObject(output, o._1)
          kryo.writeObject(output, o._2)
        })(outputHandler)
      case None =>
        // noop
    }
  }

  def exportId(vId : I, index : ThreadId, workerId : WorkerId, asyncSerializer: AsyncSerializer, outputHandler : ByteBuffer => Future[ByteBuffer])(implicit exeContext : ExecutionContext)  = {
    asyncSerializer.serialize[I](index, workerId, vId, (kryo, output, o) => {
      kryo.writeObject(output, vId)
    })(outputHandler)
  }

  def exportEdges(vId : I, index : ThreadId, workerId : WorkerId, asyncSerializer: AsyncSerializer, outputHandler : ByteBuffer => Future[ByteBuffer])(implicit exeContext : ExecutionContext)  = {
    edges(vId).foreach{ edge =>
      asyncSerializer.serialize[Edge[I,E]](index, workerId, edge, (kryo, output, o) => {
        kryo.writeObject(output, vId)
        kryo.writeObject(output, o.dst)
        if (!isNullClass) {
          kryo.writeObject(output, o.e)
        }
      })(outputHandler)
    }
  }

  def removeAllFromVertex(vId : I)
  def addVertex(vertex : I)
  def addEdge(src :I, dst : I, value : E)
  def edges(vId : I) : Iterable[Edge[I,E]]
  def mutableEdges(vId : I) : Iterable[Edge[I,E]]
  def releaseEdgesIterable(edgesIterable : Iterable[Edge[I,E]])
  def parVertices : ParIterable[I]
  def vertices : Iterable[I]
  def halted(vId : I) : Boolean
  def vertexValue(vId : I) : V
  def setVertexValue(vId : I, v : V)
  def setHalted(vId : I, halted : Boolean)
  def localNumVertices : Int
  def localNumEdges : Int
}
