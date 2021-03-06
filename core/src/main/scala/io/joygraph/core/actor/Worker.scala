package io.joygraph.core.actor

import java.lang.Thread.UncaughtExceptionHandler
import java.nio.ByteBuffer

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.UnreachableMember
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, Input, Output}
import com.typesafe.config.Config
import io.joygraph.core.actor.communication.impl.netty.{MessageReceiverNetty, MessageSenderNetty}
import io.joygraph.core.actor.messaging.impl.TrieMapMessageStore
import io.joygraph.core.actor.messaging.impl.serialized.{OHCacheMessagesStore, OpenHashMapSerializedMessageStore, TrieMapSerializedMessageStore}
import io.joygraph.core.actor.messaging.{Message, MessageStore}
import io.joygraph.core.actor.service.RequestResponseService
import io.joygraph.core.actor.state.GlobalState
import io.joygraph.core.actor.vertices.impl.TrieMapVerticesStore
import io.joygraph.core.actor.vertices.impl.serialized.{JavaHashMapSerializedVerticesStore, OHCacheSerializedVerticesStore, OpenHashMapSerializedVerticesStore, TrieMapSerializedVerticesStore}
import io.joygraph.core.actor.vertices.{VertexEdge, VerticesStore}
import io.joygraph.core.config.JobSettings
import io.joygraph.core.message._
import io.joygraph.core.message.elasticity._
import io.joygraph.core.message.superstep._
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program._
import io.joygraph.core.reader.LineProvider
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferInputStream
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.concurrency.Types
import io.joygraph.core.util.serde._
import io.joygraph.core.util.{SimplePool, _}
import io.netty.channel.Channel

import scala.concurrent.{ExecutionContext, Future}

object Worker{
  def workerWithTrieMapMessageStore[I,V,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) {
      override def initialize(): Unit = {
        super.initialize()
        verticesStore = new TrieMapVerticesStore[I,V,E](
          clazzI, clazzE, clazzV
        )
        messageStore = new TrieMapMessageStore
      }
    }
    worker.initialize()
    worker
  }

  def workerWithSerializeJavaHashMapStore[I,V,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) {
      override def initialize(): Unit = {
        super.initialize()

        verticesStore = new JavaHashMapSerializedVerticesStore[I,V,E](
          clazzI, clazzE, clazzV, jobSettings.workerCores, jobSettings.maxEdgeSize, exceptionReporter
        )
        messageStore = new OpenHashMapSerializedMessageStore(jobSettings.workerCores, jobSettings.maxFrameLength, exceptionReporter)
      }
    }
    worker.initialize()
    worker
  }

  def workerWithSerializeOpenHashMapStore[I,V,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) {
      override def initialize(): Unit = {
        super.initialize()

        log.info("Using OpenHashMapSerializedVerticesStore, OpenHashMapSerializedMessageStore")

        verticesStore = new OpenHashMapSerializedVerticesStore[I,V,E](
          clazzI, clazzE, clazzV, jobSettings.workerCores, jobSettings.maxEdgeSize, exceptionReporter
        )

        elasticVerticesStore = new OpenHashMapSerializedVerticesStore[I,V,E](
          clazzI, clazzE, clazzV, jobSettings.workerCores, jobSettings.maxEdgeSize, exceptionReporter
        )
        messageStore = new OpenHashMapSerializedMessageStore(jobSettings.workerCores, jobSettings.maxFrameLength, exceptionReporter)
        elasticMessagesStore = new OpenHashMapSerializedMessageStore(jobSettings.workerCores, jobSettings.maxFrameLength, exceptionReporter)
      }
    }
    worker.initialize()
    worker
  }

  def workerWithSerializeOHCStore[I,V,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) {
      override def initialize(): Unit = {
        super.initialize()
        log.info("Using OHCacheSerializedVerticesStore, OHCacheMessagesStore")
        verticesStore = new OHCacheSerializedVerticesStore[I,V,E](
          clazzI, clazzE, clazzV, jobSettings.workerCores, jobSettings.maxEdgeSize, exceptionReporter
        )

        elasticVerticesStore = new OHCacheSerializedVerticesStore[I,V,E](
          clazzI, clazzE, clazzV, jobSettings.workerCores, jobSettings.maxEdgeSize, exceptionReporter
        )
        messageStore = new OHCacheMessagesStore(clazzI.asInstanceOf[Class[Any]], jobSettings.workerCores, jobSettings.maxFrameLength, exceptionReporter)
        elasticMessagesStore = new OHCacheMessagesStore(clazzI.asInstanceOf[Class[Any]], jobSettings.workerCores, jobSettings.maxFrameLength, exceptionReporter)
      }
    }
    worker.initialize()
    worker
  }

  def workerWithSerializedTrieMapMessageStore[I ,V ,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) {
      override def initialize(): Unit = {
        super.initialize()
        verticesStore = new TrieMapSerializedVerticesStore[I,V,E](
          clazzI, clazzE, clazzV, partitioner, jobSettings.maxEdgeSize
        )
        messageStore = new TrieMapSerializedMessageStore(partitioner, jobSettings.maxFrameLength)
      }
    }
    worker.initialize()
    worker
  }
}

// TODO remove partitioner as argument, let master propagate partitioner
abstract class Worker[I,V,E]
(private[this] val config : Config,
 programDefinition: ProgramDefinition[String, I,V,E],
 protected[this] var partitioner : VertexPartitioner)
  extends Actor with ActorLogging with MessageCounting with Types {

  protected[this] val jobSettings = JobSettings(config)
  protected[this] var verticesStore : VerticesStore[I,V,E] = _
  protected[this] var elasticVerticesStore : VerticesStore[I,V,E] = _
  protected[this] var messageStore : MessageStore = _
  protected[this] var elasticMessagesStore : MessageStore = _

  protected[this] val exceptionReporter : (Throwable) => Unit = (t : Throwable) => {
    log.info("Sending terminate to master on exception\n" + Errors.messageAndStackTraceString(t))
    terminate()
  }

  private[this] implicit val messageHandlingExecutionContext = ExecutionContext.fromExecutor(
    ExecutionContextUtil.createForkJoinPoolWithPrefix("worker-akka-message-"),
    (t: Throwable) => {
      exceptionReporter(t)
    }
  )

  private[this] val computationNonFatalReporter: (Throwable) => Unit = (t: Throwable) => {
    exceptionReporter(t)
  }

  private[this] val computationUncaughtExceptionHandler = new UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      log.info("uncaught exception")
      computationNonFatalReporter(e)
    }
  }
  private[this] val computationFjPool = ExecutionContextUtil.createForkJoinPoolWithPrefix("compute-", jobSettings.workerCores, computationUncaughtExceptionHandler)
  private[this] val computationExecutionContext = ExecutionContext.fromExecutor(
    computationFjPool,
    computationNonFatalReporter
  )

  protected[this] val clazzI : Class[I] = programDefinition.clazzI
  protected[this] val clazzV : Class[V] = programDefinition.clazzV
  protected[this] val clazzE : Class[E] = programDefinition.clazzE
  protected[this] val voidOrUnitClass = TypeUtil.unitOrVoid(clazzE)

  private[this] var id : Option[Int] = None
  private[this] var workers : Map[Int, AddressPair] = Map.empty

  private[this] var workerPathsToIndex : Map[ActorRef, Int] = null
  private[this] var verticesBufferNew : AsyncSerializer = null
  private[this] var edgeBufferNew : AsyncSerializer = null
  private[this] var dataLoadingDeserializer : AsyncDeserializer = null

  private[this] var masterActorRef : ActorRef = null
  private[this] val vertexProgramInstance = programDefinition.program.newInstance()

  private[this] var messageSender : MessageSenderNetty = _
  private[this] var messageReceiver : MessageReceiverNetty = _

  private[this] var messagesSerializer : AsyncSerializer = null
  private[this] var messagesCombinableSerializer : CombinableAsyncSerializer[I] = null
  private[this] var messagesDeserializer : AsyncDeserializer = null

  private[this] var qapComputation : QueryAnswerVertexComputation[I,V,E,_,_,_]= null
  private[this] var querySerializer : AsyncSerializer = null
  private[this] var answerSerializer : AsyncSerializer = null
  private[this] var queryDeserializer : AsyncDeserializer = null
  private[this] var answerDeserializer : AsyncDeserializer = null

  private[this] var allHalted = true
  private[this] var currentSuperStepFunction : SuperStepFunction[I, V, E] = _
  private[this] var receivedCallback : () => Unit = _
  private[this] var messageHandler : (ThreadId, ObjectByteBufferInputStream) => Boolean = _
  private[this] var requestResponseService : RequestResponseService[I, V, E] = _

  def initialize() = {
    partitioner.init(config)
  }

  protected[this] def master() = masterActorRef

  var _state = GlobalState.NONE

  def state = _state
  def state_=(state : GlobalState.Value): Unit = {
    _state = state
  }

  private[this] val edgeSerDes = new SimplePool[EdgeDeserializer[I,E]]({
    new EdgeDeserializer[I,E](clazzI, clazzE)
  })

  private[this] val edgeSerializer = new EdgeSerializer[I,E](clazzI, clazzE)

  private[this] def _handleDataLoading(index : Int, is : ObjectByteBufferInputStream): Unit = {
    is.msgType match {
      case 0 => // edge
        edgeSerDes { implicit edgeSerDe =>
          dataLoadingDeserializer.deserialize(is, index, edgeSerDe.edgeDeserializer) { implicit edges =>
            edges.foreach(x => verticesStore.addEdge(x.src, x.dst, x.value))
          }
        }
      case 1 => // vertex
        dataLoadingDeserializer.deserialize(is, index, vertexDeserializer) { implicit vertices =>
          vertices.foreach(verticesStore.addVertex)
        }
    }
  }

  private[this] def handleNettyMessage(byteBuffer : ByteBuffer): Unit = {
    val workerId = byteBuffer.getInt()
    val is = new ObjectByteBufferInputStream(byteBuffer)
    is.msgType match {
      case -1 =>
        incrementReceived()
        receivedCallback()
      case _ =>
        val threadId = ThreadId.getMod(jobSettings.nettyWorkers)
        if (messageHandler(threadId, is)) {
          messageSender.sendAck(id.get, workerId)
        }
    }
  }

  private[this] def handleDataLoadingNetty(threadId : ThreadId, is : ObjectByteBufferInputStream): Boolean = {
    _handleDataLoading(threadId, is)
    true
  }

  private[this] def handleSuperStepNetty(threadId : ThreadId, is : ObjectByteBufferInputStream): Boolean = {
    try {
      _handleSuperStep(threadId, is)
    } catch {
      case t : Throwable => exceptionReporter(t)
        throw t
    }
  }

  private[this] val messageSerDes = new SimplePool({
    val msgSerDe = new MessageSerDe[I](clazzI)
    msgSerDe.setIncomingClass(currentIncomingMessageClass)
    msgSerDe.setOutgoingClass(currentOutgoingMessageClass)
    msgSerDe
  })
  private[this] val messageSerDeSharable = new MessageSerDe[I](clazzI)

  private[this] def _handleSuperStep(index : Int, is : ObjectByteBufferInputStream): Boolean = {
    is.msgType match {
      case 0 => // edge
        messageSerDes { implicit messageSerDe =>
          messagesDeserializer.deserialize(is, index, messageSerDe.messagePairDeserializer) { implicit dstMPairs =>
            dstMPairs.foreach(messageStore._handleMessage(index, _, clazzI, currentOutgoingMessageClass))
          }
        }
        true
      case 1 => // request
        currentSuperStepFunction match {
          case ssF: RequestResponse[I, V, E, _, _] =>
            requestResponseService.handleRequest(ssF, is)
          case ssF : QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_] =>
            qapComputation.vertexAnswer(verticesStore, queryDeserializer, is)
        }
        false
      case 2 => // response
        currentSuperStepFunction match {
          case ssF: RequestResponse[I, V, E, _, _] =>
            requestResponseService.handleResponse(ssF, is)
          case ssF : QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_] =>
            qapComputation.receiveAnswer(verticesStore, answerDeserializer, is)
        }
        false
    }
  }

  private[this] def addEdge(threadId: ThreadId, srcWorkerId: WorkerId, vertexEdge: VertexEdge[I, E]): Unit = {
    ifMessageToSelf(srcWorkerId) {
      verticesStore.addEdge(vertexEdge.src, vertexEdge.dst, vertexEdge.value)
    } {
      edgeBufferNew.serialize(threadId, srcWorkerId, vertexEdge, edgeSerializer.edgeSerializer) { implicit byteBuffer =>
        log.info(s"${id.get} sending to $srcWorkerId, size: ${byteBuffer.position()}")
        messageSender.send(id.get, srcWorkerId, byteBuffer)
      }
    }
  }

  private[this] def addVertex(threadId : ThreadId, workerId : WorkerId, vertexId : I) : Unit = {
    ifMessageToSelf(workerId) {
      verticesStore.addVertex(vertexId)
    } {
      verticesBufferNew.serialize(threadId, workerId, vertexId, vertexSerializer) { implicit byteBuffer =>
        log.info(s"${id.get} sending to $workerId, size: ${byteBuffer.position()}")
        messageSender.send(id.get, workerId, byteBuffer)
      }
    }
  }

  private[this] def addDataNoVerticesData(vertexEdge : VertexEdge[I,E]) = {
    // TODO pick correct pool
    val threadId = ThreadId.getMod(computationFjPool.getParallelism)
    val srcWorkerId : Int = partitioner.destination(vertexEdge.src)
    addEdge(threadId, srcWorkerId, vertexEdge)

    val dstWorkerId : Int = partitioner.destination(vertexEdge.dst)
    addVertex(threadId, dstWorkerId, vertexEdge.dst)
  }

  private[this] val DATA_LOADING_OPERATION : PartialFunction[Any, Unit] = {
    case PrepareLoadData() =>
      log.info(s"$id: prepare load data!~ $state")
      // TODO create KryoFactory
      this.edgeBufferNew = new AsyncSerializer(0, () => new Kryo(), jobSettings.maxFrameLength)
      this.verticesBufferNew = new AsyncSerializer(1, () => new Kryo(), jobSettings.maxFrameLength)
      this.dataLoadingDeserializer = new AsyncDeserializer(new Kryo())
      sender() ! true
    case LoadDataWithVertices(verticesPath, verticesPathPosition, verticesPathLength, path, start, length) => Future {
      log.info(s"${id.get} reading $path $start $length")
      log.info(s"${id.get} reading $verticesPath $verticesPathPosition $verticesPathLength")

      val lineProvider: LineProvider = jobSettings.inputDataLineProvider.newInstance()
      IOUtil.splits(length, jobSettings.workerCores, start).par.foreach {
        case (position, splitLength) =>
          log.info(s"${id.get} parsing $path $position $splitLength")
          lineProvider.read(config, path, position, splitLength) {
            val vertexEdge = VertexEdge[I,E](null.asInstanceOf[I], null.asInstanceOf[I], null.asInstanceOf[E])
            val vertexEdgeReversed = VertexEdge[I,E](null.asInstanceOf[I], null.asInstanceOf[I], null.asInstanceOf[E])
            _.foreach { l =>
              programDefinition.edgeParser(l, vertexEdge)
              val threadId = ThreadId.getMod(computationFjPool.getParallelism)
              val srcWorkerId: Int = partitioner.destination(vertexEdge.src)
              addEdge(threadId, srcWorkerId, vertexEdge)
              if (!jobSettings.isDirected) {
                val dstWorkerId: Int = partitioner.destination(vertexEdge.dst)
                vertexEdgeReversed.src = vertexEdge.dst
                vertexEdgeReversed.dst = vertexEdge.src
                vertexEdgeReversed.value = vertexEdge.value
                addEdge(threadId, dstWorkerId, vertexEdgeReversed)
              }
            }
          }
      }

      IOUtil.splits(verticesPathLength, jobSettings.workerCores, verticesPathPosition).par.foreach {
        case (position, splitLength) =>
          lineProvider.read(config, verticesPath, position, splitLength) {
            log.info(s"${id.get} parsing $verticesPath $position $splitLength")
            _.foreach { l =>
              val vertexId = programDefinition.vertexParser(l)
              val threadId = ThreadId.getMod(computationFjPool.getParallelism)
              val workerId: Int = partitioner.destination(vertexId)
              addVertex(threadId, workerId, vertexId)
            }
          }
      }
      verticesBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, workerId : Int) =>
        log.info(s"${id.get} final sending to $workerId, size: ${byteBuffer.position()}")
        messageSender.send(id.get, workerId, byteBuffer)
      })

      edgeBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, workerId : Int) =>
        log.info(s"${id.get} final sending to $workerId, size: ${byteBuffer.position()}")
        messageSender.send(id.get, workerId, byteBuffer)
      })
      sendingComplete()
      loadingCompleteTrigger()
    }(computationExecutionContext)
    case LoadData(path, start, length) =>
      log.info(s"${id.get} reading $path $start $length")
      Future {
        val lineProvider: LineProvider = jobSettings.inputDataLineProvider.newInstance()
        IOUtil.splits(length, jobSettings.workerCores, start).par.foreach {
          case (position, splitLength) =>
            log.info(s"${id.get} parsing $path $position $splitLength")
            val vertexEdge = VertexEdge[I,E](null.asInstanceOf[I], null.asInstanceOf[I], null.asInstanceOf[E])
            val vertexEdgeReversed = VertexEdge[I,E](null.asInstanceOf[I], null.asInstanceOf[I], null.asInstanceOf[E])
            lineProvider.read(config, path, position, splitLength) {
              _.foreach { l =>
                programDefinition.edgeParser(l, vertexEdge)
                addDataNoVerticesData(vertexEdge)
                if (!jobSettings.isDirected) {
                  vertexEdgeReversed.src = vertexEdge.dst
                  vertexEdgeReversed.dst = vertexEdge.src
                  vertexEdgeReversed.value = vertexEdge.value
                  addDataNoVerticesData(vertexEdgeReversed)
                }
              }
            }
        }

        verticesBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, workerId : Int) =>
          log.info(s"${id.get} final sending to $workerId, size: ${byteBuffer.position()}")
          messageSender.send(id.get, workerId, byteBuffer)
        })

        edgeBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, workerId : Int) =>
          log.info(s"${id.get} final sending to $workerId, size: ${byteBuffer.position()}")
          messageSender.send(id.get, workerId, byteBuffer)
        })

        sendingComplete()
        loadingCompleteTrigger()
      }(computationExecutionContext)
    case AllLoadingComplete() =>
      log.info(s"serializing edges: ${edgeBufferNew.timeSpent.get() / 1000}s " +
        s"vertices: ${verticesBufferNew.timeSpent.get() / 1000}s")
      master() ! LoadingComplete(id.get, verticesStore.localNumVertices, verticesStore.localNumEdges)
  }

  protected[this] def loadingCompleteTrigger(): Unit = {
    if (doneAllSentReceived) {
      synchronized {
        if (doneAllSentReceived) {
          resetSentReceived()
          log.info(s"$id phase ended")
          master() ! AllLoadingComplete()
        }
      }
    }
  }

  protected[this] def superStepCompleteTrigger(): Unit = {
    if (doneAllSentReceived) {
      synchronized {
        if (doneAllSentReceived) {
          resetSentReceived()
          log.info(s"$id phase ended")
          vertexProgramInstance match {
            case aggregatable: Aggregatable => {
              aggregatable.printAggregatedValues()
              master() ! SuperStepComplete(Some(aggregatable.aggregators()))
            }
            case _ =>
              master() ! SuperStepComplete()
          }
        }
      }
    } else {
      log.info(s"${id} Triggered but not completed : $numMessagesSent $numMessagesReceived")
    }
  }

  private[this] def currentIncomingMessageClass : Class[_] = currentSuperStepFunction match {
    case pregelSuperStepFunction : PregelSuperStepFunction[I,V,E,_,_] => pregelSuperStepFunction.clazzIn
    case qap : QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_] => qap.classM
  }

  private[this] def currentOutgoingMessageClass : Class[_] = currentSuperStepFunction match {
    case pregelSuperStepFunction : PregelSuperStepFunction[I,V,E,_,_] => pregelSuperStepFunction.clazzOut
    case qap : QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_] => null // there is no outgoing messaging after the step
  }

  private[this] def ifMessageToSelf(workerId : Int)(any : => Unit)(otherwise : => Unit) : Unit = {
    if (workerId == id.get) {
      any
    } else {
      otherwise
    }
  }

  private[this] def prepareSuperStep(superStep : Int) : Unit = {
    currentSuperStepFunction = vertexProgramInstance.currentSuperStepFunction(superStep)

    vertexProgramInstance match {
      case aggregatable : Aggregatable => {
        // cache saved values
        aggregatable.saveAggregatedValues()

        // possibly resetting aggregator
        aggregatable.aggregators().values.foreach(_.workerPrepareStep())
      }
      case _ =>
        // noop
    }
    // preSuperStep can still access saved aggregated values
    vertexProgramInstance.preSuperStep()

    messageSerDes.foreach(_.setIncomingClass(currentIncomingMessageClass))
    messageSerDeSharable.setIncomingClass(currentIncomingMessageClass)

    currentSuperStepFunction match {
      case pregelSuperStepFunction : PregelSuperStepFunction[I,V,E,_,_] =>
        messageSerDes.foreach(_.setOutgoingClass(currentOutgoingMessageClass))
        messageSerDeSharable.setOutgoingClass(currentOutgoingMessageClass)
        // set current deserializer
        messageStore.setReusableIterableFactory({
          val reusableIterable = new ReusableIterable[Any] {
            override protected[this] def deserializeObject(): Any = messageSerDeSharable.incomingMessageDeserializer(_kryo, _input)
          }
          // TODO 4096 max message size should be retrieved somewhere else,
          // prior to this it was retrieved from KryoSerialization
          reusableIterable.input(new ByteBufferInput(jobSettings.maxFrameLength))
          reusableIterable
        })
      case queryAnswerProcessSuperStepFunction : QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_] =>
        val superStepFunctionPool = new SimplePool[QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_]](qapSuperStepFunctionFactory(superStep))
        // create queries
        qapComputation = new QueryAnswerVertexComputation(
          superStepFunctionPool,
          clazzI,
          queryAnswerProcessSuperStepFunction.clazzS,
          queryAnswerProcessSuperStepFunction.clazzG,
          currentIncomingMessageClass,
          verticesStore,
          messageStore,
          workers,
          messageSender,
          querySerializer,
          answerSerializer,
          partitioner,
          id.get,
          computationFjPool.getParallelism
        )
    }
  }

  private[this] def barrier(superStep : Int): Unit = {
    prepareSuperStep(superStep)
  }

  private[this] def initializeProgram(program : NewVertexProgram[I,V,E], totalNumVertices : Long, totalNumEdges : Long) = {
    program.load(config)
    program.totalNumVertices(totalNumVertices)
    program.totalNumEdges(totalNumEdges)
    program match {
      case aggregatable: Aggregatable => {
        aggregatable.workerInitializeAggregators()
      }
      case _ =>
    }
  }

  private[this] def qapSuperStepFunctionFactory(superStep : Int) : QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_] = {
    // TODO this flow should be described.
    val clonedVertexInstance = vertexProgramInstance.newInstance(config)
    clonedVertexInstance match {
      case aggregatable : Aggregatable =>
        aggregatable.copyReferencesFrom(vertexProgramInstance.asInstanceOf[Aggregatable])
      case _ =>
      // noop
    }
    clonedVertexInstance.preSuperStep()
    // TODO this flow should be described, aggregated values are available

    val ssF = clonedVertexInstance.currentSuperStepFunction(superStep).asInstanceOf[QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_]]
    ssF
  }

  private[this] def pregelSuperStepFunctionFactory(superStep : Int): PregelSuperStepFunction[I, V, E, _, _] = {
    // TODO this flow should be described.
    val clonedVertexInstance = vertexProgramInstance.newInstance(config)
    clonedVertexInstance match {
      case aggregatable : Aggregatable =>
        aggregatable.copyReferencesFrom(vertexProgramInstance.asInstanceOf[Aggregatable])
      case _ =>
      // noop
    }
    clonedVertexInstance.preSuperStep()
    // TODO this flow should be described, aggregated values are available

    val ssF = clonedVertexInstance.currentSuperStepFunction(superStep).asInstanceOf[PregelSuperStepFunction[I,V,E,_,_]]
    ssF match {
      case requestResponse : RequestResponse[I,V,E,_,_] =>
        requestResponse.setRequestFunc(
          (req, dst) =>
            requestResponseService.sendRequest(ssF, req, dst)
        )
      case _ =>
      // noop
    }
    ssF match {
      case combinable : Combinable[Any @unchecked] =>
        implicit val c = combinable
        val message = Message[I](null.asInstanceOf[I], null)
        ssF.messageSenders(
          (m, dst) => {
            val workerId = partitioner.destination(dst)
            val threadId = ThreadId.getMod(computationFjPool.getParallelism)
            ifMessageToSelf(workerId) {
              // could combine but.. it's not over the network
              message.dst = dst
              message.msg = m
              messageStore._handleMessage(threadId, message, clazzI, currentOutgoingMessageClass)
            } {
              messageSerDes { implicit messageSerDe =>
                messagesCombinableSerializer.serialize(threadId, workerId, dst, m, messageSerDe.messageSerializer, messageSerDe.messageDeserializer) { implicit byteBuffer =>
                  messageSender.send(id.get, workerId, byteBuffer)
                }
              }
            }
          },
          (v, m) => {
            v.edges.foreach {
              case Edge(dst, _) =>
                val workerId = partitioner.destination(dst)
                val threadId = ThreadId.getMod(computationFjPool.getParallelism)
                ifMessageToSelf(workerId) {
                  // could combine but.. it's not over the network
                  message.dst = dst
                  message.msg = m
                  messageStore._handleMessage(threadId, message, clazzI, currentOutgoingMessageClass)
                } {
                  messageSerDes { implicit messageSerDe =>
                    messagesCombinableSerializer.serialize(threadId, workerId, dst, m, messageSerDe.messageSerializer, messageSerDe.messageDeserializer) { implicit byteBuffer =>
                      messageSender.send(id.get, workerId, byteBuffer)
                    }
                  }
                }
            }
          })
      case _ =>
        val message = Message[I](null.asInstanceOf[I], null)
        ssF.messageSenders(
          (m, dst) => {
            val workerId = partitioner.destination(dst)
            val threadId = ThreadId.getMod(computationFjPool.getParallelism)
            message.dst = dst
            message.msg = m

            ifMessageToSelf(workerId) {
              messageStore._handleMessage(threadId, message, clazzI, currentOutgoingMessageClass)
            } {
              messagesSerializer.serialize(threadId, workerId, message, messageSerDeSharable.messagePairSerializer) { implicit byteBuffer =>
                log.info(s"${id.get} sending message to $workerId of size ${byteBuffer.position()} of type ${m.getClass}")
                messageSender.send(id.get, workerId, byteBuffer)
              }
            }
          },
          (v, m) => {
            v.edges.foreach {
              case Edge(dst, _) =>
                val workerId = partitioner.destination(dst)
                val threadId = ThreadId.getMod(computationFjPool.getParallelism)
                message.dst = dst
                message.msg = m
                ifMessageToSelf(workerId) {
                  messageStore._handleMessage(threadId, message, clazzI, currentOutgoingMessageClass)
                } {
                  messagesSerializer.serialize(threadId, workerId, message, messageSerDeSharable.messagePairSerializer) { implicit byteBuffer =>
                    log.info(s"${id.get} sending message to $workerId of size ${byteBuffer.position()} of type ${m.getClass}")
                    messageSender.send(id.get, workerId, byteBuffer)
                  }
                }
            }
          })
    }
    ssF
  }

  def superStepLocalComplete() = {
    master() ! SuperStepLocalComplete()
  }

  private[this] val RUN_SUPERSTEP : PartialFunction[Any, Unit] = {
    case PrepareSuperStep(numVertices, numEdges) =>
      initializeProgram(vertexProgramInstance, numVertices, numEdges)
      prepareSuperStep(0)
      sender() ! true
    case DoBarrier(superStep, globalAggregatedValues) => {
      // update global
     vertexProgramInstance match {
        case aggregatable : Aggregatable => {
          println("dem global aggergateors " +  globalAggregatedValues.map(x => x._1 + " " + x._2.value).foldLeft("")(_ + "" + _))
          aggregatable.globalAggregators(globalAggregatedValues)
        }
        case _ => // noop
      }
      barrier(superStep)
      messageStore.messagesOnBarrier()
      master() ! BarrierComplete()
    }
    case RunSuperStep(superStep) =>
      Future {
        log.info(s"Running superstep $superStep")

        allHalted = currentSuperStepFunction match {
          case pregelSuperstepFunction : PregelSuperStepFunction[I,V,E,_,_] =>
            val superStepFunctionPool = new SimplePool[PregelSuperStepFunction[I,V,E,_,_]](pregelSuperStepFunctionFactory(superStep))
            val vertexComputation: PregelVertexComputation[I, V, E] = new PregelVertexComputation(currentIncomingMessageClass, messageStore, superStepFunctionPool)
            val computationHalted = verticesStore.computeVertices(vertexComputation)

            currentSuperStepFunction match {
              case combinable : Combinable[_] =>
                messagesCombinableSerializer.sendNonEmptyByteBuffers{ case (byteBuffer : ByteBuffer, targetWorkerId : Int) =>
                  messageSender.send(id.get, targetWorkerId, byteBuffer)
                }
              case _ =>
                messagesSerializer.sendNonEmptyByteBuffers { case (byteBuffer : ByteBuffer, targetWorkerId : Int) =>
                  log.info(s"${id.get} sending final message to $targetWorkerId of size ${byteBuffer.position()} of type ${currentOutgoingMessageClass}")
                  messageSender.send(id.get, targetWorkerId, byteBuffer)
                }
            }
            computationHalted
          case queryAnswerProcessSuperStepFunction : QueryAnswerProcessSuperStepFunction[I,V,E,_,_,_] =>
            // create queries
            verticesStore.createQueries(qapComputation)
            qapComputation.flushQueries()

            // await computation fulfillment
            qapComputation.await();
        }

        // trigger oncomplete
        vertexProgramInstance.onSuperStepComplete()
        superStepLocalComplete()
        sendingComplete()
        superStepCompleteTrigger()
      }(computationExecutionContext) // TODO move computationcontext to VerticesStores
        .recover{
          case t : Throwable => computationNonFatalReporter(t)
        }
    case AllSuperStepComplete() =>
      if (allHalted && messageStore.emptyNextMessages) {
        log.info("Don't do next step! ")
        sender() ! DoNextStep(false)
      } else {
        log.info("Do next step! ")
        sender() ! DoNextStep(true)
      }
    case GetNumActiveVertices() =>
      val senderRef = sender()
      senderRef ! NumActiveVertices(id.get, verticesStore.countActiveVertices(messageStore, currentOutgoingMessageClass))
    case QapFlushAnswers(workerId) =>
      qapComputation.flushAnswers(workerId)
    case QapQueryProcessed(workerId, numProcessed) =>
      qapComputation.processedQueriesNotification(workerId, numProcessed)
  }

  private[this] def currentReceive : PartialFunction[Any, Unit] = _currentReceive
  private[this] def currentReceive_=(that : PartialFunction[Any, Unit]) = _currentReceive = that

  private[this] val POST_SUPERSTEP_OPERATION : PartialFunction[Any, Unit] = {
    case DoOutput(outputPath) =>
      Future {
        log.info(s"Writing output to ${outputPath}")
        jobSettings.outputDataLineWriter.newInstance().write(config, outputPath, (outputStream) => {

          val v : Vertex[I,V,E] = new VertexImpl[I,V,E] {
            override def addEdge(dst: I, e: E): Unit = {} // noop
          }
          verticesStore.vertices.foreach { vId =>
            v.load(vId, verticesStore.vertexValue(vId), verticesStore.edges(vId))
            programDefinition.outputWriter(v, outputStream)
          }
        })
        log.info(s"Done writing output ${outputPath}")
        master() ! DoneOutput()
      }
  }

  private[this] var elasticDeserializer : AsyncDeserializer = _

  private[this] def prepareElasticStep() = {
    elasticDeserializer = new AsyncDeserializer(new Kryo())
  }


  private[this] def handleElasticNetty(threadId : ThreadId, is : ObjectByteBufferInputStream) : Boolean = {
    try {
      is.msgType match {
        case _ =>
          val threadId = ThreadId.getMod(jobSettings.nettyWorkers)
          elasticVerticesStore.importVerticesStoreData(threadId, is,
            elasticDeserializer,
            elasticDeserializer,
            elasticDeserializer,
            elasticDeserializer
          )
          messageSerDes { implicit messageSerDe =>
            elasticMessagesStore.importNextMessagesData(threadId, is, messagesDeserializer, messageSerDe.messagePairDeserializer, clazzI, currentOutgoingMessageClass)
          }
      }
    } catch {
      case t : Throwable => exceptionReporter(t)
    }
    true
  }

  /**
    * uses computeExecutionContext
    */
  def distributeVerticesEdgesAndMessages(prevWorkers: Map[Int, AddressPair], nextWorkers: Map[Int, AddressPair], partitioner : VertexPartitioner) = {
    this.partitioner = partitioner
//    // dump it bra
//    File.makeTemp("dumpOrig").writeAll({
//      val otherVertices = verticesStore.vertices.flatMap{ v =>
//        val dst = partitioner.destination(v)
//        if (dst != id.get) {
//          Some(v)
//        } else {
//          None
//        }
//      }
//      otherVertices.toIndexedSeq.sortBy(_.toString.toLong).map{
//        x =>
//          s"$x: ${verticesStore.halted(x)} ${verticesStore.vertexValue(x)} ${verticesStore.mutableEdges(x)}"
//      }
//    } : _*)
    println(s"number of vertices to distribute ${verticesStore.vertices.count(partitioner.destination(_) != id.get)}}")

    val haltedAsyncSerializer = new AsyncSerializer(0, () => new Kryo, jobSettings.maxFrameLength)
    val idAsyncSerializer = new AsyncSerializer(1, () => new Kryo, jobSettings.maxFrameLength)
    val valueAsyncSerializer = new AsyncSerializer(2, () => new Kryo, jobSettings.maxFrameLength)
    val edgesAsyncSerializer = new AsyncSerializer(3, () => new Kryo, jobSettings.maxFrameLength)
    val messagesAsyncSerializer = new AsyncSerializer(4, () => new Kryo, jobSettings.maxFrameLength)

    val outputHandler = (byteBuffer : ByteBuffer, workerId : Int) => {
      log.info(s"${id.get} sending elastic buffer ${byteBuffer.position()} to ${workerId}")
      messageSender.send(id.get, workerId, byteBuffer)
    }

    // TODO change this, it's not so nice to tweak the ingoing and outgoing classes
    messageStore.setReusableIterableFactory({
      val reusableIterable = new ReusableIterable[Any] {
        override protected[this] def deserializeObject(): Any = messageSerDeSharable.messageDeserializer(_kryo, _input)
      }
      // TODO 4096 max message size should be retrieved somewhere else,
      // prior to this it was retrieved from KryoSerialization
      reusableIterable.input(new ByteBufferInput(4096))
      reusableIterable
    })

    verticesStore.distributeVertices(
      id.get,
      haltedAsyncSerializer,
      idAsyncSerializer,
      valueAsyncSerializer,
      edgesAsyncSerializer,
      partitioner,
      outputHandler,
      messageStore,
      messagesAsyncSerializer,
      currentOutgoingMessageClass
    )

    haltedAsyncSerializer.sendNonEmptyByteBuffers{
      case (byteBuffer : ByteBuffer, workerId : Int) =>
        outputHandler(byteBuffer, workerId)
    }
    idAsyncSerializer.sendNonEmptyByteBuffers{
      case (byteBuffer : ByteBuffer, workerId : Int) =>
        outputHandler(byteBuffer, workerId)
    }
    valueAsyncSerializer.sendNonEmptyByteBuffers{
      case (byteBuffer : ByteBuffer, workerId : Int) =>
        outputHandler(byteBuffer, workerId)
    }
    edgesAsyncSerializer.sendNonEmptyByteBuffers {
      case (byteBuffer : ByteBuffer, workerId : Int) =>
        outputHandler(byteBuffer, workerId)
    }

    messagesAsyncSerializer.sendNonEmptyByteBuffers {
      case (byteBuffer : ByteBuffer, workerId : Int) =>
        outputHandler(byteBuffer, workerId)
    }

    log.info("Force running GC")
    sys.runtime.gc()
    log.info("GC complete")
    sendingComplete()
    elasticCompleteTrigger()
  }

  private[this] def elasticCompleteTrigger() = {
    if (doneAllSentReceived) {
      synchronized {
        if (doneAllSentReceived) {
          resetSentReceived()
          log.info(s"$id elastic phase ended")
          master() ! ElasticityComplete()
        }
      }
    } else {
      log.info(s"${id} elastic Triggered but not completed : $numMessagesSent $numMessagesReceived")
    }
  }

  private[this] val ELASTIC_OPERATION : PartialFunction[Any, Unit] = {
    case NewPartitioner(newPartitioner) =>
      this.partitioner = newPartitioner
      sender() ! true
    case ElasticDistribute(prevWorkers, nextWorkers, newPartitioner) =>
      assert(nextWorkers.nonEmpty)
      Future {
        this.workers = nextWorkers
        distributeVerticesEdgesAndMessages(prevWorkers, nextWorkers, newPartitioner)
      }(computationExecutionContext)
        .recover{
          case t : Throwable => computationNonFatalReporter(t)
        }
  }

  private[this] def addWorkers(senderRef : ActorRef, newWorkers : Map[Int, AddressPair]) = {
    this.workers ++= newWorkers
    this.workerPathsToIndex = this.workers.mapValues(_.actorRef).map(_.swap)
    FutureUtil.callbackOnAllComplete{ // TODO handle failure
    val destinationAddressPairs = newWorkers.map{case (index, addressPair) => (index, (addressPair.nettyAddress.host, addressPair.nettyAddress.port))}
      messageSender.connectToAll(destinationAddressPairs)
    } {
      senderRef ! true
    }
  }

  private[this] def removeWorkers(senderRef : ActorRef, workers : Map[Int, AddressPair]) = {
    this.workers = this.workers.filterNot(x => workers.contains(x._1))
    FutureUtil.callbackOnAllComplete{
      messageSender.disconnectFromAll(workers.keys)
    } {
      senderRef ! true
    }
  }

  private[this] def nettyReceiverException(ch : Channel, t : Throwable) : Unit = {
    Option(ch) match {
      case Some(_) =>
        log.info("Error channel " + ch.remoteAddress() + " " + ch.localAddress())
      case None =>
    }
    log.info("Error receiver handler\n" + Errors.messageAndStackTraceString(t))
    terminate()
  }

  private[this] def nettySenderException(ch : Channel, t : Throwable) : Unit = {
    Option(ch) match {
      case Some(_) =>
        log.info("Error channel " + ch.remoteAddress() + " " + ch.localAddress())
      case None =>
    }
    log.info("Error sender handler\n" + Errors.messageAndStackTraceString(t))
    terminate()
  }

  private[this] val BASE_OPERATION : PartialFunction[Any, Unit] = {
    case UnreachableMember(member) =>
      log.info(s"${member.address} ${master().path.address}")
      if (member.address == master().path.address) {
        context.system.terminate()
      }
    case MasterAddress(address) =>
      masterActorRef = address
      sender() ! true
    case InitNewWorker(address, currentSuperStep, numVertices, numEdges) =>
      masterActorRef = address
      initializeProgram(vertexProgramInstance, numVertices, numEdges)
      prepareSuperStep(currentSuperStep)
      sender() ! NewWorker()
    case WorkerId(workerId) =>
      log.info(s"worker id: $workerId")
      val senderRef = sender()
      this.id = Option(workerId)

      // TODO kryoFactory instead of new Kryo
      messagesSerializer = new AsyncSerializer(0, () => new Kryo(), jobSettings.maxFrameLength)
      messagesCombinableSerializer = new CombinableAsyncSerializer[I](0, () => new Kryo(), jobSettings.maxFrameLength, vertexSerializer)
      messagesDeserializer = new AsyncDeserializer(new Kryo())

      querySerializer = new AsyncSerializer(1, () => new Kryo(), jobSettings.maxFrameLength)
      answerSerializer = new AsyncSerializer(2, () => new Kryo(), jobSettings.maxFrameLength)
      queryDeserializer = new AsyncDeserializer(new Kryo())
      answerDeserializer = new AsyncDeserializer(new Kryo())

      messageSender = new MessageSenderNetty(this, jobSettings.nettyWorkers)
      messageSender.setOnExceptionHandler(nettySenderException)

      requestResponseService = new RequestResponseService[I,V,E](clazzI, verticesStore, messageSender, workerId, partitioner, computationExecutionContext)

      messageReceiver = new MessageReceiverNetty(jobSettings.nettyWorkers, jobSettings.maxFrameLength)
      messageReceiver.setReceiverExceptionReporter(nettyReceiverException)
      messageReceiver.setOnReceivedMessage(handleNettyMessage)

      messageReceiver.connect().foreach { success =>
        if (success) {
          // TODO Cluster(context.system) is not the nicest way to do this
          val addressPair = AddressPair(self, NettyAddress(Cluster(context.system).selfAddress.host.get, messageReceiver.port))
          senderRef ! addressPair
          log.info(s"$workerId sent self: $addressPair ?!?!")
        } else {
          log.error("Could not listen on ANY port")
        }
      }
    case NewWorkerMap(newWorkers) =>
      val senderRef = sender()
      log.info(s"new workers: $newWorkers")
      addWorkers(senderRef, newWorkers)
    case ElasticCleanUp() =>
      log.info("elastic cleanup start")
      val senderRef = sender()
      // we remove the vertices that have been sent away in the ElasticDistribute step
      verticesStore.removeDistributedVerticesEdgesAndMessages(id.get, partitioner)
      messageStore.removeNextMessages(id.get, partitioner)

      log.info("adding bufferStore")
      // merge the elastic store and clean it up
      verticesStore.addAll(elasticVerticesStore)
      messageStore.addAllNextMessages(elasticMessagesStore)
      log.info("adding bufferstore complete")

      log.info("elastic cleanup stop")
      senderRef ! true
    case ElasticRemoval(workersToBeRemoved) =>
      val senderRef = sender()
      log.info(s"workers to be removed: $workersToBeRemoved")
      removeWorkers(senderRef, workersToBeRemoved)
    case WorkerMap(workers) =>
      val senderRef = sender()
      log.info(s"workers: $workers")
      addWorkers(senderRef, workers)
    case State(newState) =>
      log.info(s"Preparing to move from state $state -> $newState")
      state = newState
      state match {
        case GlobalState.LOAD_DATA =>
          // set receiver
          messageHandler = handleDataLoadingNetty
          receivedCallback = () => loadingCompleteTrigger()
          currentReceive = (BASE_OPERATION :: DATA_LOADING_OPERATION :: Nil).reduceLeft(_ orElse _)
        case GlobalState.SUPERSTEP =>
          messageHandler = handleSuperStepNetty
          receivedCallback = () => superStepCompleteTrigger()
          currentReceive = (BASE_OPERATION :: RUN_SUPERSTEP :: Nil).reduceLeft(_ orElse _)
        case GlobalState.POST_SUPERSTEP =>
          currentReceive = (BASE_OPERATION :: POST_SUPERSTEP_OPERATION :: Nil).reduceLeft(_ orElse _)
        case GlobalState.ELASTIC_DISTRIBUTE =>
          prepareElasticStep()
          messageHandler = handleElasticNetty
          receivedCallback = () => elasticCompleteTrigger()
          currentReceive = (BASE_OPERATION :: ELASTIC_OPERATION :: Nil).reduceLeft(_ orElse _)
        case GlobalState.NONE =>
      }
      context.become(currentReceive, true)
      log.info(s"Set state to $newState")
      sender() ! true
  }

  private[this] def terminate(): Unit = {
    // send terminatation to Master
    master() ! InitiateTermination()
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    //terminate netty
    messageSender.shutDown()
    messageReceiver.shutdown()

    log.info("Terminating worker")
    context.system.terminate()
  }

  private[this] var _currentReceive : PartialFunction[Any, Unit] = BASE_OPERATION

  override def receive = currentReceive

  private[this] def vertexDeserializer(kryo : Kryo, input : Input) : I = {
    kryo.readObject(input, clazzI)
  }

  private[this] def vertexSerializer(kryo: Kryo, output : Output, v : I) : Unit = {
    kryo.writeObject(output, v)
  }
}
