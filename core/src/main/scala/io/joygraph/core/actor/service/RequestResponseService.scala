package io.joygraph.core.actor.service

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import com.esotericsoftware.kryo.Kryo
import io.joygraph.core.actor.communication.impl.netty.MessageSenderNetty
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program.{PregelSuperStepFunction, RequestResponse, VertexImpl}
import io.joygraph.core.util.SimplePool
import io.joygraph.core.util.buffers.streams.bytebuffer.{ObjectByteBufferInputStream, ObjectByteBufferOutputStream}

import scala.concurrent.ExecutionContext

class RequestResponseService[I,V,E]
(clazzI : Class[I],
 verticesStore : VerticesStore[I,V,E],
 messageSender: MessageSenderNetty,
 selfWorkerId : Int,
 partitioner : VertexPartitioner,
 implicit val executionContext : ExecutionContext
) {

  private[this] val atomicInteger = new AtomicInteger(0)
  private[this] val waitingResponses = new ConcurrentHashMap[Int, CountDownLatch]()
  private[this] val responses = new ConcurrentHashMap[Int, Any]()
  private[this] val requestMessageType : Byte = 1
  private[this] val responseMessageType : Byte = 2

  private[this] val requestOutputStreams = new SimplePool({
    new ObjectByteBufferOutputStream(requestMessageType, 1 * 1024 * 1024)
  })

  private[this] val responseOutputStreams = new SimplePool({
    new ObjectByteBufferOutputStream(responseMessageType, 1 * 1024 * 1024)
  })

  private[this] val kryoPool = new SimplePool(new Kryo)

  def handleRequest(ssF : RequestResponse[I,V,E,_,_], is: ObjectByteBufferInputStream): Unit = {
    kryoPool { implicit kryo =>
      val msgId = is.readInt()
      val workerId = is.readInt()
      val targetVertexId = kryo.readObject(is, clazzI)
      val reqObject = kryo.readObject(is, ssF.reqClazz)
      val vertex = new VertexImpl[I,V,E] {
        override def addEdge(dst: I, e: E): Unit = {
          // noop
        }
      }
      val vValue = verticesStore.vertexValue(targetVertexId)

      val response = verticesStore.explicitlyScopedEdges(targetVertexId) { implicit vEdges =>
        vertex.load(targetVertexId, vValue, vEdges)
        ssF.responseProxy(vertex, reqObject)
      }

      val os = responseOutputStreams.borrow()
      os.writeInt(msgId)
      kryo.writeObject(os, response)
      os.writeCounter()

      messageSender.sendNoAck(selfWorkerId, workerId, os.handOff()).foreach {
        _ =>
          os.resetOOS()
          responseOutputStreams.release(os)
      }
    }
  }

  def handleResponse(ssF : RequestResponse[I,V,E,_,_], is: ObjectByteBufferInputStream): Unit = {
    val kryo = new Kryo
    val msgId = is.readInt()
    val response = kryo.readObject(is, ssF.resClazz)
    responses.put(msgId, response)
    val lock = waitingResponses.remove(msgId)
    lock.countDown()
  }

  def sendRequest(currentSuperStepFunction : PregelSuperStepFunction[I,V,E,_,_], request : Any, dst : I) : Any = {
    val workerId = partitioner.destination(dst)

    if (workerId == selfWorkerId) {
      currentSuperStepFunction match {
        case ssF : RequestResponse[I,V,E,_,_] =>
          val vertex = new VertexImpl[I,V,E] {
            override def addEdge(dst: I, e: E): Unit = {
              // noop
            }
          }
          val vValue = verticesStore.vertexValue(dst)
          verticesStore.explicitlyScopedEdges(dst) { implicit vEdges =>
            vertex.load(dst, vValue, vEdges)
            ssF.responseProxy(vertex, request)
          }

        case _ =>
          throw new IllegalArgumentException()
      }
    } else {
      val msgId = atomicInteger.incrementAndGet()
      val os = requestOutputStreams.borrow()
      os.writeInt(msgId)
      os.writeInt(selfWorkerId)
      kryoPool{ implicit kryo =>
        kryo.writeObject(os, dst)
        kryo.writeObject(os, request)
      }
      os.writeCounter()

      // await
      val lock = new CountDownLatch(1)
      waitingResponses.put(msgId, lock)

      messageSender.sendNoAck(selfWorkerId, workerId, os.handOff()).foreach{ _ =>
        os.resetOOS()
        requestOutputStreams.release(os)
      }

      lock.await()
      responses.remove(msgId)
    }
  }

}
