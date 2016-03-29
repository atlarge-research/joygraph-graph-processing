package io.joygraph.core.actor

import java.nio.ByteBuffer

import akka.actor._
import akka.util.ByteString
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

  private[this] implicit val messageHandlingExecutionContext = ExecutionContext.fromExecutor(
    ExecutionContextUtil.createForkJoinPoolWithPrefix("worker-akka-message-"),
    (t: Throwable) => {
      log.info("Terminating on exception " + Errors.messageAndStackTraceString(t))
      terminate()
    }
  )

  private[this] val computationExecutionContext = ExecutionContext.fromExecutor(
    ExecutionContextUtil.createForkJoinPoolWithPrefix("compute-"),
    (t: Throwable) => {
      log.info("Terminating on exception " + Errors.messageAndStackTraceString(t))
      terminate()
    }
  )

  protected[this] val clazzI : Class[I] = programDefinition.clazzI
  protected[this] val clazzV : Class[V] = programDefinition.clazzV
  protected[this] val clazzE : Class[E] = programDefinition.clazzE

  private[this] var id : Option[Int] = None
  private[this] var workers : Map[Int, AddressPair] = Map.empty
  private[this] var workerPathsToIndex : Map[ActorRef, Int] = null

  private[this] val jobSettings = JobSettings(config)
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
    val index = byteBuffer.getInt()
    _handleDataLoading(index, byteBuffer)
    workers(index).actorRef ! Received()
  }

  private[this] def handleSuperStepNetty(byteBuffer: ByteBuffer) = {
    try {
      val index = byteBuffer.getInt()
      _handleSuperStep(index, byteBuffer)
      workers(index).actorRef ! Received()
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

  private[this] def handleSuperStepAkka(senderRef : ActorRef, byteString : ByteString) = {
    Future {
      // we only copy the buffer because Akka does not allow mutability by design
      val byteBuffer = ByteBuffer.allocate(byteString.size)
      val numCopied = byteString.copyToBuffer(byteBuffer)
      byteBuffer.flip()
      println(byteBuffer.order())
      val index = workerPathsToIndex(senderRef)
      println(s"received from $index size: ${byteString.size} numCopied: $numCopied")
      _handleSuperStep(index, byteBuffer)
      println(s"$id sending reply to $index")
      senderRef ! Received()
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
            val index : Int = partitioner.destination(src)
            val index2 : Int = partitioner.destination(dst)
            if (index == id.get) {
              addEdge(src, dst, value)
            } else {
              edgeBufferNew.serialize(index, (src, dst, value), edgeSerializer) { implicit byteBuffer =>
                println(s"- sending to $index, size: ${byteBuffer.position()}")
                messageSender.send(id.get, index, byteBuffer)
              }
            }
            if (index2 == id.get) {
              addVertex(dst)
            } else {
              verticesBufferNew.serialize(index2, dst, vertexSerializer) { implicit byteBuffer =>
                println(s"- sending to $index2, size: ${byteBuffer.position()}")

                messageSender.send(id.get, index2, byteBuffer)
              }
            }
          }
        }

        verticesBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, index : Int) =>
          println(s"final sending to $index, size: ${byteBuffer.position()}")
          messageSender.send(id.get, index, byteBuffer)
        })

        edgeBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, index : Int) =>
          println(s"final sending to $index, size: ${byteBuffer.position()}")
          messageSender.send(id.get, index, byteBuffer)
        })
        sendingComplete()
        loadingCompleteTrigger()
      }
    case Received() =>
      incrementReceived()
      loadingCompleteTrigger()
    case AllLoadingComplete() =>
      println(s"taking edges: ${edgeBufferNew.timeSpentTaking} " +
        s"taking vertices: ${verticesBufferNew.timeSpentTaking}")
      println(s"serializing edges: ${edgeBufferNew.timeSpent.get() / 1000}s " +
        s"vertices: ${verticesBufferNew.timeSpent.get() / 1000}s")
      master() ! LoadingComplete(id.get, numVertices, numEdges)
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

  private[this] def prepareSuperStep(superStep : Int) : Unit = {
    currentSuperStepFunction = vertexProgramInstance.currentSuperStepFunction(superStep)

    println(s"current INOUT $currentIncomingMessageClass $currentOutgoingMessageClass")

    currentSuperStepFunction match {
      case combinable : Combinable[Any @unchecked] =>
        implicit val c = combinable
        currentSuperStepFunction.messageSenders(
          (m, dst) => {
            val index = partitioner.destination(dst)
            messagesCombinableSerializer.serialize(index, dst, m, messageSerializer, messageDeserializer) { implicit byteBuffer =>
              messageSender.send(id.get, index, byteBuffer)
            }
          },
          (v, m) => {
            v.edges.foreach {
              case Edge(dst, _) =>
                val index = partitioner.destination(dst)
                messagesCombinableSerializer.serialize(index, dst, m, messageSerializer, messageDeserializer) { implicit byteBuffer =>
                  messageSender.send(id.get, index, byteBuffer)
                }
            }
          })
      case _ =>
        currentSuperStepFunction.messageSenders(
          (m, dst) => {
            val index = partitioner.destination(dst)
            messagesSerializer.serialize(index, (dst, m), messagePairSerializer) { implicit byteBuffer =>
              messageSender.send(id.get, index, byteBuffer)
            }
          },
          (v, m) => {
            v.edges.foreach {
              case Edge(dst, _) =>
                val index = partitioner.destination(dst)
                messagesSerializer.serialize(index, (dst, m), messagePairSerializer) { implicit byteBuffer =>
                  messageSender.send(id.get, index, byteBuffer)
                }
            }
          })
    }

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

    // last chance to retrieve aggregator
    vertexProgramInstance.preSuperStep()
    // possibly resetting aggregator
    vertexProgramInstance match {
      case aggregatable : Aggregatable => {
        aggregatable.aggregators().values.foreach(_.workerPrepareStep())
      }
      case _ =>
        // noop
    }
  }

  private[this] def barrier(superStep : Int): Unit = {
    prepareSuperStep(superStep)
  }

  private[this] val RUN_SUPERSTEP : PartialFunction[Any, Unit] = {
    case PrepareSuperStep(numVertices, numEdges) =>
      vertexProgramInstance match {
        case program: NewVertexProgram[I, V, E] =>
          program.load(config)
          program.totalNumVertices(numVertices)
          program.totalNumEdges(numEdges)
          vertexProgramInstance match {
            case aggregatable: Aggregatable => {
              aggregatable.workerInitializeAggregators()
            }
            case _ =>
          }
        case _ =>
      }

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
      Future{
        log.info(s"Running superstep $superStep")
        def addEdgeVertex : (I, I, E) => Unit = addEdge

        val v : Vertex[I,V,E] = new VertexImpl[I,V,E] {
          override def addEdge(dst: I, e: E): Unit = {
            addEdgeVertex(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
          }
        }

        allHalted = true

        vertices.foreach { vId =>
          val vMessages = messages(vId, currentIncomingMessageClass)
          val vHalted = halted(vId)
          val hasMessage = vMessages.nonEmpty
          if (!vHalted || hasMessage) {
            val value : V = vertexValue(vId)
            val edgesIterable : Iterable[Edge[I,E]] = edges(vId)
            val mutableEdgesIterable : Iterable[Edge[I,E]] = mutableEdges(vId)
            v.load(vId, value, edgesIterable, mutableEdgesIterable)

            val hasHalted = currentSuperStepFunction(v, vMessages)
            setVertexValue(vId, v.value)
            setHalted(vId, hasHalted)

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
            messagesCombinableSerializer.sendNonEmptyByteBuffers{ case (byteBuffer : ByteBuffer, index : Int) =>
              messageSender.send(id.get, index, byteBuffer)
            }
          case _ =>
            messagesSerializer.sendNonEmptyByteBuffers { case (byteBuffer : ByteBuffer, index : Int) =>
              messageSender.send(id.get, index, byteBuffer)
            }
        }

        // trigger oncomplete
        vertexProgramInstance.onSuperStepComplete()

        sendingComplete()
        superStepCompleteTrigger()
      }(computationExecutionContext)
    case byteString : ByteString =>
      handleSuperStepAkka(sender(), byteString)
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
      val index = byteBuffer.getInt() // reads the node Id which is prepended during netty send
      val is = new ObjectByteBufferInputStream(byteBuffer)

      importVerticesStoreData(index, is,
        elasticGrowthDeserializer,
        elasticGrowthDeserializer,
        elasticGrowthDeserializer,
        elasticGrowthDeserializer
      )

      importCurrentMessagesData(index, is, messagesDeserializer, messagePairDeserializer, clazzI, currentOutgoingMessageClass)
      workers(index).actorRef ! Received()
    } catch {
      case t : Throwable => t.printStackTrace()
    }
  }

  /**
    *
    * @param prevWorkers
    * @param nextWorkers
    */
  def distributeVerticesEdgesAndMessages(prevWorkers: Map[Int, AddressPair], nextWorkers: Map[Int, AddressPair]) = {
    // update partitioner
    partitioner.numWorkers(nextWorkers.size)

    // new workers
    val verticesToBeRemoved = ArrayBuffer.empty[I]
    val newWorkersMap = nextWorkers.map{
      case (index, addressPair) =>
        index -> !prevWorkers.contains(index)
    }

    val haltedAsyncSerializer = new AsyncSerializer(0, () => new Kryo)
    val idAsyncSerializer = new AsyncSerializer(1, () => new Kryo)
    val valueAsyncSerializer = new AsyncSerializer(2, () => new Kryo)
    val edgesAsyncSerializer = new AsyncSerializer(3, () => new Kryo)
    val messagesAsyncSerializer = new AsyncSerializer(4, () => new Kryo)

    val outputHandler = (byteBuffer : ByteBuffer, index : Int) => {
      messageSender.send(id.get, index, byteBuffer)
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

    vertices.foreach{ vId =>
      val index = partitioner.destination(vId)
      if (newWorkersMap(index)) {
        verticesToBeRemoved += vId
        exportHaltedState(vId, index, haltedAsyncSerializer, (buffer) => outputHandler(buffer, index))
        exportId(vId, index, idAsyncSerializer, (buffer) => outputHandler(buffer, index))
        exportValue(vId, index, valueAsyncSerializer, (buffer) => outputHandler(buffer, index))
        exportEdges(vId, index, edgesAsyncSerializer, (buffer) => outputHandler(buffer, index))
        exportAndRemoveMessages(vId, currentOutgoingMessageClass, index, messagesAsyncSerializer, (buffer) => outputHandler(buffer, index))
      }
    }

    haltedAsyncSerializer.sendNonEmptyByteBuffers{
      case (byteBuffer : ByteBuffer, index : Int) =>
        outputHandler(byteBuffer, index)
    }
    idAsyncSerializer.sendNonEmptyByteBuffers{
      case (byteBuffer : ByteBuffer, index : Int) =>
        outputHandler(byteBuffer, index)
    }
    valueAsyncSerializer.sendNonEmptyByteBuffers{
      case (byteBuffer : ByteBuffer, index : Int) =>
        outputHandler(byteBuffer, index)
    }
    edgesAsyncSerializer.sendNonEmptyByteBuffers {
      case (byteBuffer : ByteBuffer, index : Int) =>
        outputHandler(byteBuffer, index)
    }

    messagesAsyncSerializer.sendNonEmptyByteBuffers {
      case (byteBuffer : ByteBuffer, index : Int) =>
        outputHandler(byteBuffer, index)
    }

    verticesToBeRemoved.foreach(removeAllFromVertex)

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

      messageReceiver = new MessageReceiverNetty
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
