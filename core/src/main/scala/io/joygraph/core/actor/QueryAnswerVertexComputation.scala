package io.joygraph.core.actor

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import io.joygraph.core.actor.communication.impl.netty.MessageSenderNetty
import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program.{Edge, QueryAnswerProcessSuperStepFunction, Vertex, VertexImpl}
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferInputStream
import io.joygraph.core.util.serde.{AsyncDeserializer, AsyncSerializer}
import io.joygraph.core.util.{SimplePool, ThreadId}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.reflect.ClassTag


// computation intensity can be tweaked by adding more/less netty workers (to compute answers and do computations)
// with this the network buffers can be enlarged per dst worker.
class QueryAnswerVertexComputation[I,V,E,S,G,M]
(superStepFunctionPool : SimplePool[QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_]],
 clazzI : Class[I],
 clazzS : Class[S],
 clazzG : Class[G],
 clazzM : Class[M],
 workerVerticesStore : VerticesStore[I,V,E],
 messageStore : MessageStore,
 workers : Map[Int, AddressPair],
 messageSender: MessageSenderNetty,
 querySerializer : AsyncSerializer,
 answerSerializer : AsyncSerializer,
 vertexPartitioner: VertexPartitioner,
 selfWorkerId : Int,
 parallelism : Int
){

  private[this] val _superStepFunctionPool = superStepFunctionPool.asInstanceOf[SimplePool[QueryAnswerProcessSuperStepFunction[I,V,E,S,G,M]]]
  private[this] val messagesToBeReceived = new ConcurrentHashMap[I, AtomicInteger]()
  private[this] val answersLatch = new ConcurrentHashMap[I, CountDownLatch]()
  private[this] val vertexAnswers = new ConcurrentHashMap[I, VolatileArray[G]]()
  private[this] var allHalted = true
  private[this] val waitObject = new CountDownLatch(1)
  @volatile private[this] var queriesSent = false

  private[this] val threadLocalReusableVertex : ThreadLocal[Vertex[I,V,E]] = new ThreadLocal[Vertex[I, V, E]] {
    override def initialValue(): Vertex[I, V, E] = new VertexImpl[I,V,E] {
      override def addEdge(dst: I, e: E): Unit = throw new UnsupportedOperationException()
    }
  }

  private[this] def serializeQuery(kryo : Kryo, kryoOutput : Output, q : QueryMessage[I,S]) = {
    kryo.writeObject(kryoOutput, q.src)
    kryo.writeObject(kryoOutput, q.dst)
    kryo.writeObject(kryoOutput, q.message)
  }

  private[this] def serializeAnswer(kryo : Kryo, kryoOutput : Output, a : AnswerMessage[I,G]) = {
    kryo.writeObject(kryoOutput, a.dst)
    kryo.writeObject(kryoOutput, a.message)
  }

  private[this] def deserializeQuery(kryo : Kryo, kryoInput : Input, q : QueryMessage[I,S]) : QueryMessage[I,S] = {
    q.src = kryo.readObject(kryoInput, clazzI)
    q.dst = kryo.readObject(kryoInput, clazzI)
    q.message = kryo.readObject(kryoInput, clazzS)
    q
  }

  private[this] def deserializerAnswer(kryo : Kryo, kryoInput : Input, a : AnswerMessage[I,G]) : AnswerMessage[I,G] = {
    a.dst = kryo.readObject(kryoInput, clazzI)
    a.message = kryo.readObject(kryoInput, clazzG)
    a
  }

  private[this] var _allMessagesSentCounter : scala.collection.Map[Int, Long] = _
  private[this] val _allMessagesReceivedCounter = TrieMap.empty[Int, AtomicLong]

  private[this] def ifMessageToSelf(workerId : Int)(any : => Unit)(otherwise : => Unit) : Unit = {
    if (workerId == selfWorkerId) {
      any
    } else {
      otherwise
    }
  }

  def flushQueries() = {
    // TODO flushing won't work with just 1 workercore
    // flush queries
    querySerializer.sendNonEmptyByteBuffers {
      case (byteBuffer, workerId) =>
        messageSender.sendNoAck(selfWorkerId, workerId, byteBuffer)
    }
    _allMessagesSentCounter = querySerializer.getAllMessagesSent()
    querySerializer.resetMessageSentCounters()
    queriesSent = true
    _allMessagesSentCounter.keys.foreach(processedAllQueriesTrigger)
  }

  def flushAnswers(workerId : Int) = {
    answerSerializer.flushNonEmptyByteBuffer(workerId) {
      case (byteBuffer, _) =>
        messageSender.sendNoAck(selfWorkerId, workerId, byteBuffer)
    }
  }

  private[this] def notifyToFlushAnswer(workerId : Int) = {
    workers(workerId).actorRef ! QapFlushAnswers(selfWorkerId)
  }

  private[this] def processedAllQueriesTrigger(srcWorkerId : Int): Unit = {
    if (queriesSent) {
      if (_allMessagesSentCounter(srcWorkerId) == _allMessagesReceivedCounter.getOrElseUpdate(srcWorkerId, new AtomicLong(0)).get()) {
        synchronized {
          _allMessagesSentCounter.get(srcWorkerId) match {
            case Some(x) =>
              if (x == _allMessagesReceivedCounter(srcWorkerId).get()) {
                _allMessagesReceivedCounter(srcWorkerId).set(0)
                notifyToFlushAnswer(srcWorkerId)
              }
            case None =>
              // noop
          }
        }
      }
    }
  }

  def processedQueriesNotification(srcWorkerId : Int, numProcessed : Long): Unit = {
    _allMessagesReceivedCounter.getOrElseUpdate(srcWorkerId, new AtomicLong(0)).addAndGet(numProcessed)
    processedAllQueriesTrigger(srcWorkerId)
  }

  def vertexQuery(vId : I, verticesStore: VerticesStore[I,V,E], reusableVertex : Vertex[I,V,E]): Unit = {
    val messages = messageStore.messages(vId, clazzM)

    verticesStore.halted(vId) && messages.nonEmpty match {
      case false =>
        // get vertex impl
        val reusableQuery : QueryMessage[I,S] = QueryMessage(null.asInstanceOf[I], null.asInstanceOf[I], null.asInstanceOf[S])
        val value: V = verticesStore.vertexValue(vId)
        val edgesIterable: Iterable[Edge[I, E]] = verticesStore.edges(vId)
        val mutableEdges = verticesStore.mutableEdges(vId)
        reusableVertex.load(vId, value, edgesIterable, mutableEdges)
        _superStepFunctionPool { ssF =>
          val threadId = ThreadId.getMod(parallelism)

          val queries = ssF.query(reusableVertex, messages)
          val numQueries = queries.size
          // set value
          verticesStore.setVertexValue(vId, reusableVertex.value)

          if (queries.nonEmpty) {
            // release before sending away queries
            verticesStore.releaseEdgesIterable(edgesIterable)
            verticesStore.releaseEdgesIterable(mutableEdges)

            // set sent-received to establish a happens-before relationship
            messagesToBeReceived.put(vId, new AtomicInteger(numQueries))
            vertexAnswers.put(vId, new VolatileArray[G](numQueries, clazzG))
            answersLatch.put(vId, new CountDownLatch(numQueries))

            queries.foreach {
              case query @ (dst, message) =>
                val dstWorkerId = vertexPartitioner.destination(dst)
                ifMessageToSelf(dstWorkerId) {
                  vertexAnswerLocal(threadId, vId, query, verticesStore, reusableVertex)
                } {
                  reusableQuery.src = vId
                  reusableQuery.dst = dst
                  reusableQuery.message = message
                  querySerializer.serialize(threadId, dstWorkerId, reusableQuery, serializeQuery) { byteBuffer =>
                    messageSender.sendNoAck(selfWorkerId, dstWorkerId, byteBuffer)
                  }
                }
            }
          } else {
            // do computation
            vertexComputation(reusableVertex, verticesStore, Iterable.empty)
            // release after computation queries
            verticesStore.releaseEdgesIterable(edgesIterable)
            verticesStore.releaseEdgesIterable(mutableEdges)
          }
        }
      case _ => // noop
    }
    messageStore.releaseMessages(messages, clazzM)
  }

  def vertexAnswerLocal(threadId : Int, srcId : I, query : (I, S), partitionedVerticesStore: VerticesStore[I,V,E], reusableVertex : Vertex[I,V,E]) : Unit = {
    val (vId, message) = query
    val value: V = workerVerticesStore.vertexValue(vId)

    val answer = workerVerticesStore.explicitlyScopedEdges(vId) { edgesIterable =>
      // get vertex impl
      reusableVertex.load(vId, value, edgesIterable)
      _superStepFunctionPool { ssF =>
        ssF.answer(reusableVertex, message)
      }
    }

    val answers = vertexAnswers.get(srcId)
    val index = messagesToBeReceived.get(srcId).decrementAndGet()
    answers(index) = answer // write should be volatile
    val answerLatch = answersLatch.get(srcId)
    answerLatch.countDown()
    if (index == 0) { // there is only ONE index 0 due to atomicInteger
      answerLatch.await()

      // now we can pass the answers to the vertexComputation
      vertexComputation(srcId, partitionedVerticesStore, reusableVertex, answers)

      // clean up messagesReceived etc
      messagesToBeReceived.remove(srcId)
      answersLatch.remove(srcId)
      vertexAnswers.remove(srcId)

      if (vertexAnswers.size() == 0) { // we are done computing
        waitObject.countDown()
      }
    }
  }

  private[this] def computeVertexAnswer(threadId : Int, dstWorkerId : Int, query :  QueryMessage[I,S], verticesStore: VerticesStore[I,V,E], reusableVertex : Vertex[I,V,E]) = {
    val vId = query.dst
    val value: V = verticesStore.vertexValue(vId)

    verticesStore.explicitlyScopedEdges(vId) { edgesIterable =>
      // get vertex impl
      reusableVertex.load(vId, value, edgesIterable)
        _superStepFunctionPool { ssF =>
          val answer = ssF.answer(reusableVertex, query.message)
          val queryAnswer = new AnswerMessage[I, G](query.src, answer)
          answerSerializer.serialize(threadId, dstWorkerId, queryAnswer, serializeAnswer) { byteBuffer =>
            messageSender.sendNoAck(selfWorkerId, dstWorkerId, byteBuffer)
          }
        }

    }
  }

  def vertexAnswer(verticesStore: VerticesStore[I,V,E], queryDeserializer : AsyncDeserializer, is : ObjectByteBufferInputStream) : Unit = {
    val reusableQuery : QueryMessage[I,S] = QueryMessage(null.asInstanceOf[I], null.asInstanceOf[I], null.asInstanceOf[S])
    val reusableVertex = threadLocalReusableVertex.get()
    val queriesProcessed = mutable.OpenHashMap.empty[Int, LongCounter]
    ThreadId(parallelism) { threadId =>
      queryDeserializer.deserialize(is, threadId, (kryo, input) => deserializeQuery(kryo, input, reusableQuery)) {
        _.foreach {
          query =>
            val dstWorkerId = vertexPartitioner.destination(query.src)
            computeVertexAnswer(threadId, dstWorkerId, query, verticesStore, reusableVertex)
            queriesProcessed.getOrElseUpdate(dstWorkerId, new LongCounter()).increment()
        }
      }
    }
    queriesProcessed.foreach{
      case (workerId, counter) =>
        workers(workerId).actorRef ! QapQueryProcessed(selfWorkerId, counter.count)
    }
  }

  def receiveAnswer(verticesStore : VerticesStore[I,V,E], answerDeserializer : AsyncDeserializer, is : ObjectByteBufferInputStream) : Unit = {
    val reusableAnswer :  AnswerMessage[I,G] = AnswerMessage(null.asInstanceOf[I], null.asInstanceOf[G])
    val reusableVertex = threadLocalReusableVertex.get()
    ThreadId(parallelism) { threadId =>
      answerDeserializer.deserialize(is, threadId, (kryo, input) => deserializerAnswer(kryo, input, reusableAnswer)) {
        _.foreach { answer =>
          val vId = answer.dst
          val answers = vertexAnswers.get(vId)
          val index = messagesToBeReceived.get(vId).decrementAndGet()
          answers(index) = answer.message // write should be volatile
          val answerLatch = answersLatch.get(vId)
          answerLatch.countDown()
          if (index == 0) { // there is only ONE index 0 due to atomicInteger
            answersLatch.get(vId).await()

            // now we can pass the answers to the vertexComputation
            vertexComputation(vId, verticesStore, reusableVertex, answers)

            // clean up messagesReceived etc
            messagesToBeReceived.remove(vId)
            answersLatch.remove(vId)
            vertexAnswers.remove(vId)

            if (vertexAnswers.size() == 0) { // we are done computing
              waitObject.countDown()
            }
          }
        }
      }
    }

  }

  private[this] def vertexComputation(v : Vertex[I,V,E], verticesStore: VerticesStore[I,V,E], answers : Iterable[G]) : Unit = {
    val halted : Boolean = _superStepFunctionPool { ssF =>
      ssF.process(v, answers)
    }
    verticesStore.setVertexValue(v.id, v.value)
    verticesStore.setHalted(v.id, halted)
    if (!halted) {
      allHalted = false
    }
  }

  def vertexComputation(vId: I, verticesStore: VerticesStore[I,V,E], reusableVertex : Vertex[I,V,E], answers : Iterable[G]) : Unit = {
    val value: V = verticesStore.vertexValue(vId)
    verticesStore.explicitlyScopedEdges(vId) { edgesIterable =>
      // get vertex impl
      reusableVertex.load(vId, value, edgesIterable)
      vertexComputation(reusableVertex, verticesStore, answers)
    }
  }

  def await(): Boolean = {
    System.out.println(s"What are we waiting for?: ${vertexAnswers.size()} vertices")
    waitObject.await()
    allHalted
  }
}

/**
  * Volatile array to ensure entries are there on iteration.
  */
class VolatileArray[T]
(private[this] val size : Int, clazzT : Class[T]) extends Iterable[T] {
  private[this] val underlying = Array.ofDim[T](size)(ClassTag(clazzT))
  @volatile private[this] var m = 0

  // read
  def apply(i : Int): T = {
    m;
    underlying(i)
  }

  def update(i : Int, x : T) : Unit = {
    underlying(i) = x
    m = 0
  }

  override def iterator: Iterator[T] = underlying.iterator // TODO could be unsafe as not volatile read
}
class LongCounter {
  private[this] var counter = 0L

  def count = counter
  def increment() = counter += 1
  def incrementBy(x : Long) = counter += x
}
case class QapQueryProcessed(myWorkerId : Int, numProcessed : Long)
case class QapFlushAnswers(myWorkerId : Int)
case class QueryMessage[I,S](var src: I, var dst : I, var message : S)
case class AnswerMessage[I,G](var dst : I, var message : G)
