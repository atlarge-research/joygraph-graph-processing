package io.joygraph.core.actor

import java.lang.Thread.UncaughtExceptionHandler
import java.nio.ByteBuffer

import akka.actor._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, Input, Output}
import com.typesafe.config.Config
import io.joygraph.core.actor.communication.impl.netty.{MessageReceiverNetty, MessageSenderNetty}
import io.joygraph.core.actor.messaging.MessageStore
import io.joygraph.core.actor.messaging.impl.TrieMapMessageStore
import io.joygraph.core.actor.messaging.impl.serialized.TrieMapSerializedMessageStore
import io.joygraph.core.actor.state.GlobalState
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.actor.vertices.impl.TrieMapVerticesStore
import io.joygraph.core.actor.vertices.impl.serialized.TrieMapSerializedVerticesStore
import io.joygraph.core.config.JobSettings
import io.joygraph.core.message._
import io.joygraph.core.message.elasticity._
import io.joygraph.core.message.superstep._
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program._
import io.joygraph.core.reader.LineProvider
import io.joygraph.core.util._
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferInputStream
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.serde.{AsyncDeserializer, AsyncSerializer, CombinableAsyncSerializer}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

object Worker{
  def workerWithTrieMapMessageStore[I,V,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) with TrieMapMessageStore with TrieMapVerticesStore[I,V,E]
    worker.initialize()
    worker
  }

  def workerWithSerializedTrieMapMessageStoreWithVerticesStore[I,V,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) with TrieMapSerializedMessageStore with TrieMapVerticesStore[I,V,E]
    worker.initialize()
    worker
  }

  def workerWithTrieMapMessageStoreWithSerializedVerticesStore[I,V,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) with TrieMapMessageStore with TrieMapSerializedVerticesStore[I,V,E]
    worker.initialize()
    worker
  }

  def workerWithSerializedTrieMapMessageStore[I ,V ,E]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E],
   partitioner : VertexPartitioner
  ): Worker[I,V,E] = {
    val worker = new Worker[I,V,E](config, programDefinition, partitioner) with TrieMapSerializedMessageStore with TrieMapSerializedVerticesStore[I,V,E]
    worker.initialize()
    worker
    //    new Worker[I,V,E,M](config, parser, clazz, partitioner) with TrieMapMessageStore[I,M] with TrieMapSerializedVerticesStore[I,V,E]
  }
}

abstract class Worker[I,V,E]
(private[this] val config : Config,
 programDefinition: ProgramDefinition[String, I,V,E],
 protected[this] var partitioner : VertexPartitioner)
  extends Actor with ActorLogging with MessageCounting with MessageStore with VerticesStore[I,V,E] {

  private[this] val jobSettings = JobSettings(config)

  private[this] implicit val messageHandlingExecutionContext = ExecutionContext.fromExecutor(
    ExecutionContextUtil.createForkJoinPoolWithPrefix("worker-akka-message-"),
    (t: Throwable) => {
      log.info("Terminating on exception\n" + Errors.messageAndStackTraceString(t))
      terminate()
    }
  )

  private[this] val computationNonFatalReporter: (Throwable) => Unit = (t: Throwable) => {
    log.info("Terminating on exception\n" + Errors.messageAndStackTraceString(t))
    terminate()
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
  private[this] var allHalted = true
  private[this] var currentSuperStepFunction : SuperStepFunction[I,V,E,_,_] = _

  def initialize() = {
    partitioner.init(config)
  }

  protected[this] def master() = masterActorRef

  var _state = GlobalState.NONE

  def state = _state
  def state_=(state : GlobalState.Value): Unit = {
    _state = state
  }

  private[this] def _handleDataLoading(index : Int, byteBuffer : ByteBuffer): Unit = {
    val is = new ObjectByteBufferInputStream(byteBuffer)
    is.msgType match {
      case 0 => // edge
        dataLoadingDeserializer.deserialize(is, index, edgeDeserializer){ implicit edges =>
          edges.foreach(x => addEdge(x._1, x._2, x._3))
        }
      case 1 => // vertex
        dataLoadingDeserializer.deserialize(is, index, vertexDeserializer) { implicit vertices =>
          vertices.foreach(addVertex)
        }
    }
  }

  private[this] def handleDataLoadingNetty(byteBuffer : ByteBuffer): Unit = {
    val workerId = byteBuffer.getInt()
    val threadId = ThreadId.getMod(jobSettings.nettyWorkers)
    _handleDataLoading(threadId, byteBuffer)
    workers(workerId).actorRef ! Received()
  }

  private[this] def handleSuperStepNetty(byteBuffer: ByteBuffer) = {
    try {
      val workerId = byteBuffer.getInt()
      val threadId = ThreadId.getMod(jobSettings.nettyWorkers)
      log.info(s"${id.get} received superstep message from $workerId with size ${byteBuffer.remaining()} of type ${currentOutgoingMessageClass}")
      _handleSuperStep(threadId, byteBuffer)
      workers(workerId).actorRef ! Received()
    } catch { case t : Throwable => t.printStackTrace()
    }
  }

  private[this] def _handleSuperStep(index : Int, byteBuffer: ByteBuffer) = {
    val is = new ObjectByteBufferInputStream(byteBuffer)
    is.msgType match {
      case 0 => // edge
        messagesDeserializer.deserialize(is, index, messagePairDeserializer){ implicit dstMPairs =>
          dstMPairs.foreach(_handleMessage(index, _, clazzI, currentOutgoingMessageClass))
        }
    }
  }

  private[this] def addData(src :I, dst : I, value : E) = {
    val srcWorkerId : Int = partitioner.destination(src)
    val dstWorkerId : Int = partitioner.destination(dst)
    // TODO pick correct pool
    val threadId = ThreadId.getMod(computationFjPool.getParallelism)
    ifMessageToSelf(srcWorkerId) {
      addEdge(src, dst, value)
    } {
      edgeBufferNew.serialize(threadId, srcWorkerId, (src, dst, value), edgeSerializer) { implicit byteBuffer =>
        println(s"- sending to $srcWorkerId, size: ${byteBuffer.position()}")
        messageSender.send(id.get, srcWorkerId, byteBuffer)
      }
    }
    ifMessageToSelf(dstWorkerId) {
      addVertex(dst)
    } {
      verticesBufferNew.serialize(threadId, dstWorkerId, dst, vertexSerializer) { implicit byteBuffer =>
        println(s"- sending to $dstWorkerId, size: ${byteBuffer.position()}")
        messageSender.send(id.get, dstWorkerId, byteBuffer)
      }
    }
  }

  private[this] val DATA_LOADING_OPERATION : PartialFunction[Any, Unit] = {
    case PrepareLoadData() =>
      println(s"$id: prepare load data!~ $state")
      // TODO create KryoFactory
      this.edgeBufferNew = new AsyncSerializer(0, () => new Kryo())
      this.verticesBufferNew = new AsyncSerializer(1, () => new Kryo())
      this.dataLoadingDeserializer = new AsyncDeserializer(new Kryo())
      sender() ! true
    case LoadData(path, start, length) =>
      Future {
        val lineProvider : LineProvider = jobSettings.inputDataLineProvider.newInstance()
        lineProvider.read(config, path, start, length) {
          _.foreach{ l =>
            val (src, dst, value) = programDefinition.inputParser(l)
            addData(src, dst, value)
            if (!jobSettings.isDirected) {
              addData(dst, src, value)
            }
          }
        }

        verticesBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, workerId : Int) =>
          println(s"final sending to $workerId, size: ${byteBuffer.position()}")
          messageSender.send(id.get, workerId, byteBuffer)
        })

        edgeBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, workerId : Int) =>
          println(s"final sending to $workerId, size: ${byteBuffer.position()}")
          messageSender.send(id.get, workerId, byteBuffer)
        })
        sendingComplete()
        loadingCompleteTrigger()
      }
    case Received() =>
      incrementReceived()
      loadingCompleteTrigger()
    case AllLoadingComplete() =>
      println(s"serializing edges: ${edgeBufferNew.timeSpent.get() / 1000}s " +
        s"vertices: ${verticesBufferNew.timeSpent.get() / 1000}s")
      master() ! LoadingComplete(id.get, localNumVertices, localNumEdges)
  }

  protected[this] def loadingCompleteTrigger(): Unit = {
    if (doneAllSentReceived) {
      resetSentReceived()
      println(s"$id phase ended")
      master() ! AllLoadingComplete()
    }
  }

  protected[this] def superStepCompleteTrigger(): Unit = {
    if (doneAllSentReceived) {
      resetSentReceived()
      println(s"$id phase ended")
      vertexProgramInstance match {
        case aggregatable : Aggregatable => {
          aggregatable.printAggregatedValues()
          master() ! SuperStepComplete(Some(aggregatable.aggregators()))
        }
        case _ =>
          master() ! SuperStepComplete()
      }
    } else {
      log.info(s"${id} Triggered but not completed : $numMessagesSent $numMessagesReceived")
    }
  }

  private[this] def currentIncomingMessageClass : Class[_] = currentSuperStepFunction.clazzIn
  private[this] def currentOutgoingMessageClass : Class[_] = currentSuperStepFunction.clazzOut

  private[this] def ifMessageToSelf(workerId : Int)(any : => Unit)(otherwise : => Unit) : Unit = {
    if (workerId == id.get) {
      any
    } else {
      otherwise
    }
  }
  private[this] def prepareSuperStep(superStep : Int) : Unit = {
    currentSuperStepFunction = vertexProgramInstance.currentSuperStepFunction(superStep)

    println(s"current INOUT $currentIncomingMessageClass $currentOutgoingMessageClass")

    // set current deserializer
    setReusableIterablePool(new SimplePool[ReusableIterable[Any]]({
      val reusableIterable = new ReusableIterable[Any] {
        override protected[this] def deserializeObject(): Any = incomingMessageDeserializer(_kryo, _input)
      }
      // TODO 4096 max message size should be retrieved somewhere else,
      // prior to this it was retrieved from KryoSerialization
      reusableIterable.input(new ByteBufferInput(4096))
      reusableIterable
    }))

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
      messagesOnBarrier()
      master() ! BarrierComplete()
    }
    case RunSuperStep(superStep) =>
      Future {
        log.info(s"Running superstep $superStep")
        def addEdgeVertex : (I, I, E) => Unit = addEdge

        val superStepFunctionPool : SimplePool[SuperStepFunction[I,V,E,_,_]] = new SimplePool[SuperStepFunction[I, V, E, _, _]](
          {
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

            val ssF = clonedVertexInstance.currentSuperStepFunction(superStep)
            ssF match {
              case combinable : Combinable[Any @unchecked] =>
                implicit val c = combinable
                ssF.messageSenders(
                  (m, dst) => {
                    val workerId = partitioner.destination(dst)
                    val threadId = ThreadId.getMod(computationFjPool.getParallelism)
                    ifMessageToSelf(workerId) {
                      // could combine but.. it's not over the network
                      _handleMessage(threadId, (dst,m), clazzI, currentOutgoingMessageClass)
                    } {
                      messagesCombinableSerializer.serialize(threadId, workerId, dst, m, messageSerializer, messageDeserializer) { implicit byteBuffer =>
                        messageSender.send(id.get, workerId, byteBuffer)
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
                          _handleMessage(threadId, (dst,m), clazzI, currentOutgoingMessageClass)
                        } {
                          messagesCombinableSerializer.serialize(threadId, workerId, dst, m, messageSerializer, messageDeserializer) { implicit byteBuffer =>
                            messageSender.send(id.get, workerId, byteBuffer)
                          }
                        }
                    }
                  })
              case _ =>
                ssF.messageSenders(
                  (m, dst) => {
                    val workerId = partitioner.destination(dst)
                    val threadId = ThreadId.getMod(computationFjPool.getParallelism)
                    ifMessageToSelf(workerId) {
                      _handleMessage(threadId, (dst,m), clazzI, currentOutgoingMessageClass)
                    } {
                      messagesSerializer.serialize(threadId, workerId, (dst, m), messagePairSerializer) { implicit byteBuffer =>
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
                        ifMessageToSelf(workerId) {
                          _handleMessage(threadId, (dst,m), clazzI, currentOutgoingMessageClass)
                        } {
                          messagesSerializer.serialize(threadId, workerId, (dst, m), messagePairSerializer) { implicit byteBuffer =>
                            log.info(s"${id.get} sending message to $workerId of size ${byteBuffer.position()} of type ${m.getClass}")
                            messageSender.send(id.get, workerId, byteBuffer)
                          }
                        }
                    }
                  })
            }
            ssF
          }
        )

        val simpleVertexInstancePool = new SimplePool[Vertex[I,V,E]](new VertexImpl[I,V,E] {
          override def addEdge(dst: I, e: E): Unit = {
            addEdgeVertex(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
          }
        })

        allHalted = true

        parVertices.foreach { vId =>
          val vMessages = messages(vId, currentIncomingMessageClass)
          val vHalted = halted(vId)
          val hasMessage = vMessages.nonEmpty
          if (!vHalted || hasMessage) {
            val value : V = vertexValue(vId)
            val edgesIterable : Iterable[Edge[I,E]] = edges(vId)
            val mutableEdgesIterable : Iterable[Edge[I,E]] = mutableEdges(vId)
            // get vertex impl
            val v : Vertex[I,V,E] = simpleVertexInstancePool.borrow()
            v.load(vId, value, edgesIterable, mutableEdgesIterable)

            // get superstepfunction instance
            val superStepFunctionInstance = superStepFunctionPool.borrow()
            val hasHalted = superStepFunctionInstance(v, vMessages)
            // release superstepfunction instance
            superStepFunctionPool.release(superStepFunctionInstance)
            setVertexValue(vId, v.value)
            setHalted(vId, hasHalted)
            simpleVertexInstancePool.release(v)
            // release vertex impl
            if (!hasHalted) {
              allHalted = false
            }

            releaseEdgesIterable(edgesIterable)
            releaseEdgesIterable(mutableEdgesIterable)
          }
          releaseMessages(vMessages, currentIncomingMessageClass)
        }

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

        // trigger oncomplete
        vertexProgramInstance.onSuperStepComplete()

        sendingComplete()
        superStepCompleteTrigger()
      }(computationExecutionContext)
        .recover{
          case t : Throwable => computationNonFatalReporter(t)
        }
    case AllSuperStepComplete() =>
      if (allHalted && emptyNextMessages) {
        log.info("Don't do next step! ")
        sender() ! DoNextStep(false)
      } else {
        log.info("Do next step! ")
        sender() ! DoNextStep(true)
      }
    case Received() =>
      incrementReceived()
      superStepCompleteTrigger()
  }

  private[this] def messagePairDeserializer(kryo : Kryo, input : Input) : (I, Any) = {
    (kryo.readObject(input, clazzI),
      messageDeserializer(kryo, input))
  }

  private[this] def messagePairSerializer(kryo : Kryo, output : Output, o : (I, Any) ) = {
    kryo.writeObject(output, o._1)
    messageSerializer(kryo, output, o._2)
  }

  private[this] def messageSerializer(kryo : Kryo, output : Output, m : Any) = {
    kryo.writeObject(output, m)
  }

  private[this] def messageDeserializer(kryo : Kryo, input : Input) : Any = {
    kryo.readObject(input, currentOutgoingMessageClass)
  }

  private[this] def incomingMessageDeserializer(kryo : Kryo, input : Input) : Any = {
    kryo.readObject(input, currentIncomingMessageClass)
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
          vertices.foreach { vId =>
            v.load(vId, vertexValue(vId), edges(vId), mutableEdges(vId))
            programDefinition.outputWriter(v, outputStream)
          }
        })
        log.info(s"Done writing output ${outputPath}")
        master() ! DoneOutput()
      }
  }

  private[this] var elasticGrowthDeserializer : AsyncDeserializer = _

  private[this] def prepareElasticGrowStep() = {
    elasticGrowthDeserializer = new AsyncDeserializer(new Kryo())
  }


  private[this] def handleElasticGrowNetty(byteBuffer : ByteBuffer) : Unit = {
    try {
      val srcWorkerId = byteBuffer.getInt() // reads the node Id which is prepended during netty send
      val is = new ObjectByteBufferInputStream(byteBuffer)
      val threadId = ThreadId.getMod(jobSettings.nettyWorkers)
      importVerticesStoreData(threadId, is,
        elasticGrowthDeserializer,
        elasticGrowthDeserializer,
        elasticGrowthDeserializer,
        elasticGrowthDeserializer
      )

      importCurrentMessagesData(threadId, is, messagesDeserializer, messagePairDeserializer, clazzI, currentOutgoingMessageClass)
      workers(srcWorkerId).actorRef ! Received()
    } catch {
      case t : Throwable => t.printStackTrace()
    }
  }

  /**
    * uses computeExecutionContext
    */
  def distributeVerticesEdgesAndMessages(prevWorkers: Map[Int, AddressPair], nextWorkers: Map[Int, AddressPair]) = {
    // update partitioner
    partitioner.numWorkers(nextWorkers.size)

    // new workers
    val verticesToBeRemoved = TrieMap.empty[ThreadId, ArrayBuffer[I]]
    val newWorkersMap = nextWorkers.map{
      case (index, addressPair) =>
        index -> !prevWorkers.contains(index)
    }

    val haltedAsyncSerializer = new AsyncSerializer(0, () => new Kryo)
    val idAsyncSerializer = new AsyncSerializer(1, () => new Kryo)
    val valueAsyncSerializer = new AsyncSerializer(2, () => new Kryo)
    val edgesAsyncSerializer = new AsyncSerializer(3, () => new Kryo)
    val messagesAsyncSerializer = new AsyncSerializer(4, () => new Kryo)

    val outputHandler = (byteBuffer : ByteBuffer, workerId : Int) => {
      log.info(s"${id.get} sending elastic buffer ${byteBuffer.position()} to ${workerId}")
      messageSender.send(id.get, workerId, byteBuffer)
    }

    // TODO change this, it's not so nice to tweak the ingoing and outgoing classes
    setReusableIterablePool(new SimplePool[ReusableIterable[Any]]({
      val reusableIterable = new ReusableIterable[Any] {
        override protected[this] def deserializeObject(): Any = messageDeserializer(_kryo, _input)
      }
      // TODO 4096 max message size should be retrieved somewhere else,
      // prior to this it was retrieved from KryoSerialization
      reusableIterable.input(new ByteBufferInput(4096))
      reusableIterable
    }))

    parVertices.foreach{ vId =>
      val threadId = ThreadId.getMod(computationFjPool.getParallelism)
      val workerId = partitioner.destination(vId)
      if (newWorkersMap(workerId)) {
        verticesToBeRemoved.getOrElseUpdate(threadId, ArrayBuffer.empty[I]) += vId
        exportHaltedState(vId, threadId, workerId, haltedAsyncSerializer, (buffer) => outputHandler(buffer, workerId))
        exportId(vId, threadId, workerId, idAsyncSerializer, (buffer) => outputHandler(buffer, workerId))
        exportValue(vId, threadId, workerId, valueAsyncSerializer, (buffer) => outputHandler(buffer, workerId))
        exportEdges(vId, threadId, workerId, edgesAsyncSerializer, (buffer) => outputHandler(buffer, workerId))
        exportAndRemoveMessages(vId, currentOutgoingMessageClass, threadId, workerId, messagesAsyncSerializer, (buffer) => outputHandler(buffer, workerId))
      }
    }

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

    verticesToBeRemoved.par.foreach(_._2.foreach(removeAllFromVertex))
    sendingComplete()
    elasticCompleteTrigger()
  }

  private[this] def elasticCompleteTrigger() = {
    if (doneAllSentReceived) {
      resetSentReceived()
      println(s"$id elastic phase ended")
      master() ! ElasticGrowComplete()
    } else {
      log.info(s"${id} elastic Triggered but not completed : $numMessagesSent $numMessagesReceived")
    }
  }

  private[this] val GROW_OPERATION : PartialFunction[Any, Unit] = {
    case Received() =>
      incrementReceived()
      elasticCompleteTrigger()
    case ElasticGrow(prevWorkers, nextWorkers) =>
      // TODO new worker initialization
      // TODO this is done after worker initialized its listen server.
      // we assume that prevWorkers is a subset of nextWorkers
      assert(prevWorkers.size < nextWorkers.size)
      Future {
        this.workers = nextWorkers
        // TODO check messages sent/received
        // TODO link import logic
        distributeVerticesEdgesAndMessages(prevWorkers, nextWorkers)
      }(computationExecutionContext)
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

  private[this] def nettyReceiverException(t : Throwable) : Unit = {
    log.info("Error receiver handler\n" + Errors.messageAndStackTraceString(t))
    terminate()
  }

  private[this] def nettySenderException(t : Throwable) : Unit = {
    log.info("Error sender handler\n" + Errors.messageAndStackTraceString(t))
    terminate()
  }

  private[this] val BASE_OPERATION : PartialFunction[Any, Unit] = {
    case MasterAddress(address) =>
      masterActorRef = address
      sender() ! true
    case InitNewWorker(address, currentSuperStep) =>
      masterActorRef = address
      prepareSuperStep(currentSuperStep)
      sender() ! NewWorker()
    case WorkerId(workerId) =>
      log.info(s"worker id: $workerId")
      val senderRef = sender()
      this.id = Option(workerId)

      // TODO kryoFactory instead of new Kryo
      messagesSerializer = new AsyncSerializer(0, () => new Kryo())
      messagesCombinableSerializer = new CombinableAsyncSerializer[I](0, () => new Kryo(), idSerializer = vertexSerializer)
      messagesDeserializer = new AsyncDeserializer(new Kryo())

      messageSender = new MessageSenderNetty(this)
      messageSender.setOnExceptionHandler(nettySenderException)

      messageReceiver = new MessageReceiverNetty(jobSettings.nettyWorkers)
      messageReceiver.setReceiverExceptionReporter(nettyReceiverException)

      messageReceiver.connect().foreach { success =>
        if (success) {
          senderRef ! AddressPair(self, NettyAddress(messageReceiver.host, messageReceiver.port))
          log.info(s"$workerId sent self: $self ?!?!")
        } else {
          log.error("Could not listen on ANY port")
        }
      }
    case NewWorkerMap(newWorkers) =>
      val senderRef = sender()
      log.info(s"new workers: $newWorkers")
      addWorkers(senderRef, newWorkers)
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
          messageReceiver.setOnReceivedMessage(handleDataLoadingNetty)
          currentReceive = (BASE_OPERATION :: DATA_LOADING_OPERATION :: Nil).reduceLeft(_ orElse _)
        case GlobalState.SUPERSTEP =>
          messageReceiver.setOnReceivedMessage(handleSuperStepNetty)
          currentReceive = (BASE_OPERATION :: RUN_SUPERSTEP :: Nil).reduceLeft(_ orElse _)
        case GlobalState.POST_SUPERSTEP =>
          currentReceive = (BASE_OPERATION :: POST_SUPERSTEP_OPERATION :: Nil).reduceLeft(_ orElse _)
        case GlobalState.GROW_ELASTIC =>
          prepareElasticGrowStep()
          messageReceiver.setOnReceivedMessage(handleElasticGrowNetty)
          currentReceive = (BASE_OPERATION :: GROW_OPERATION :: Nil).reduceLeft(_ orElse _)
        case GlobalState.NONE =>
      }
      context.become(currentReceive, true)
      log.info(s"Set state to $newState")
      sender() ! true
    case Terminate() =>
      terminate()
  }

  private[this] def terminate() = {
    //terminate netty
    messageSender.shutDown()
    messageReceiver.shutdown()
    // terminate akka
    context.system.terminate()
  }

  private[this] var _currentReceive : PartialFunction[Any, Unit] = BASE_OPERATION

  override def receive = currentReceive

  def sendByteArray(dst : ActorRef, bytes: Array[Byte]): Unit = {
    println(s"$id sending to dst: $dst ${bytes.length}")
    dst ! bytes
    incrementSent()
  }

  private[this] def vertexDeserializer(kryo : Kryo, input : Input) : I = {
    kryo.readObject(input, clazzI)
  }

  private[this] def vertexSerializer(kryo: Kryo, output : Output, v : I) : Unit = {
    kryo.writeObject(output, v)
  }

  private[this] def edgeDeserializer(kryo : Kryo, input : Input) : (I, I, E) = {
    if (clazzE == classOf[NullClass]) {
      (kryo.readObject(input, clazzI),
        kryo.readObject(input, clazzI),
        NullClass.SINGLETON.asInstanceOf[E])
    } else {
      (kryo.readObject(input, clazzI),
        kryo.readObject(input, clazzI),
        kryo.readObject(input, clazzE))
    }
  }

  private[this] def edgeSerializer(kryo : Kryo, output : Output, o : (I, I, E)) : Unit = {
    if (clazzE == classOf[NullClass]) {
      kryo.writeObject(output, o._1)
      kryo.writeObject(output, o._2)
    } else {
      kryo.writeObject(output, o._1)
      kryo.writeObject(output, o._2)
      kryo.writeObject(output, o._3)
    }
  }
}
