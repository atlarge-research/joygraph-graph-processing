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
import io.joygraph.core.message.aggregate.Aggregators
import io.joygraph.core.message.superstep._
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program._
import io.joygraph.core.reader.LineProvider
import io.joygraph.core.util.buffers.streams.bytebuffer.ObjectByteBufferInputStream
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.serde.{AsyncDeserializer, AsyncSerializer, CombinableAsyncSerializer}
import io.joygraph.core.util.{FutureUtil, MessageCounting, SimplePool}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

object Worker{
  def workerWithTrieMapMessageStore[I ,V ,E ,M ]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E,M],
   partitioner : VertexPartitioner
  ): Worker[I,V,E,M] = {
    val worker = new Worker[I,V,E,M](config, programDefinition, partitioner) with TrieMapMessageStore with TrieMapVerticesStore[I,V,E]
    worker.initialize()
    worker
  }

  def workerWithSerializedTrieMapMessageStore[I ,V ,E ,M]
  (config: Config,
   programDefinition: ProgramDefinition[String, I,V,E,M],
   partitioner : VertexPartitioner
  ): Worker[I,V,E,M] = {
    val worker = new Worker[I,V,E,M](config, programDefinition, partitioner) with TrieMapSerializedMessageStore with TrieMapSerializedVerticesStore[I,V,E]
    worker.initialize()
    worker
    //    new Worker[I,V,E,M](config, parser, clazz, partitioner) with TrieMapMessageStore[I,M] with TrieMapSerializedVerticesStore[I,V,E]
  }
}

abstract class Worker[I ,V ,E ,M ]
(private[this] val config : Config,
 programDefinition: ProgramDefinition[String, I,V,E,M],
 protected[this] var partitioner : VertexPartitioner)
  extends Actor with ActorLogging with MessageCounting with MessageStore with VerticesStore[I,V,E] {

  // TODO use different execution contexts at different places.
  import scala.concurrent.ExecutionContext.Implicits.global

  protected[this] val clazzI : Class[I] = programDefinition.clazzI
  protected[this] val clazzV : Class[V] = programDefinition.clazzV
  protected[this] val clazzE : Class[E] = programDefinition.clazzE
  protected[this] val clazzM : Class[M] = programDefinition.clazzM

  private[this] var id : Option[Int] = None
  private[this] var workers : ArrayBuffer[AddressPair] = null
  private[this] var workerPathsToIndex : Map[ActorRef, Int] = null

  private[this] val jobSettings = JobSettings(config)
  private[this] var verticesBufferNew : AsyncSerializer = null
  private[this] var edgeBufferNew : AsyncSerializer = null
  private[this] var verticesDeserializer : AsyncDeserializer = null
  private[this] var edgesDeserializer : AsyncDeserializer = null

  private[this] var masterActorRef : ActorRef = null
  private[this] val vertexProgramInstance = programDefinition.program.newInstance()

  private[this] var messageSender : MessageSenderNetty = _
  private[this] var messageReceiver : MessageReceiverNetty = _

  private[this] var messagesSerializer : AsyncSerializer = null
  private[this] var messagesCombinableSerializer : CombinableAsyncSerializer[I,M] = null
  private[this] var messagesDeserializer : AsyncDeserializer = null
  private[this] var allHalted = true


  def initialize() = {
    partitioner.init(config)

    addPool(clazzM, new SimplePool[ReusableIterable[Any]]({
      val reusableIterable = new ReusableIterable[M] {
        override protected[this] def deserializeObject(): M = _kryo.readObject(_input, clazzM)
      }
      // TODO 4096 max message size should be retrieved somewhere else,
      // prior to this it was retrieved from KryoSerialization
      reusableIterable.input(new ByteBufferInput(4096))
      reusableIterable
    }))
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
        edgesDeserializer.deserialize(is, index, edgeDeserializer){ implicit edges =>
          edges.foreach(x => addEdge(x._1, x._2, x._3))
        }
      case 1 => // vertex
        verticesDeserializer.deserialize(is, index, vertexDeserializer) { implicit vertices =>
          vertices.foreach(addVertex)
        }
    }
  }

  private[this] def handleDataLoadingNetty(byteBuffer : ByteBuffer): Unit = {
    val index = byteBuffer.getInt()
    _handleDataLoading(index, byteBuffer)
    workers(index).actorRef ! Received()
  }

  private[this] def handleDataLoadingAkka(senderRef : ActorRef, byteString : ByteString): Unit = {
    Future {
      // we only copy the buffer because Akka does not allow mutability by design
      val byteBuffer = ByteBuffer.allocate(byteString.size)
      val numCopied = byteString.copyToBuffer(byteBuffer)
      byteBuffer.flip()
      println(byteBuffer.order())
      val index = workerPathsToIndex(senderRef)
      println(s"received from $index size: ${byteString.size} numCopied: $numCopied")
      _handleDataLoading(index, byteBuffer)
      println(s"processed from $index size: ${byteString.size} numCopied: $numCopied")
      senderRef ! Received()
    }.recover{
      case t : Throwable =>
        t.printStackTrace()
    }
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
          dstMPairs.foreach(_handleMessage(index, _, clazzI,clazzM ))
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
    }.recover{
      case t : Throwable =>
        t.printStackTrace()
    }
  }

  private[this] val DATA_LOADING_OPERATION : PartialFunction[Any, Unit] = {
    case PrepareLoadData() =>
      println(s"$id: prepare load data!~ $state")
      this.edgeBufferNew = new AsyncSerializer(0, workers.length, new Kryo())
      this.verticesBufferNew = new AsyncSerializer(1, workers.length, new Kryo())
      this.edgesDeserializer = new AsyncDeserializer(0, workers.length, new Kryo())
      this.verticesDeserializer = new AsyncDeserializer(1, workers.length, new Kryo())
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
      }.recover{
        case t : Throwable =>
          t.printStackTrace()
      }
    case byteString : ByteString =>
      handleDataLoadingAkka(sender(), byteString)
    case Received() =>
      incrementReceived()
      loadingCompleteTrigger()
    case AllLoadingComplete() =>
      println(s"taking edges: ${edgeBufferNew.timeSpentTaking} " +
        s"taking vertices: ${verticesBufferNew.timeSpentTaking}")
      println(s"serializing edges: ${edgeBufferNew.timeSpent.get() / 1000}s " +
        s"vertices: ${verticesBufferNew.timeSpent.get() / 1000}s")
      println(s"deserializing edges: ${edgesDeserializer.timeSpent.get() / 1000}s " +
        s"vertices: ${verticesDeserializer.timeSpent.get() / 1000}s")
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
      master() ! SuperStepComplete()
    } else {
      log.info(s"${id} Triggered but not completed : $numMessagesSent $numMessagesReceived")
    }
  }

  private[this] val RUN_SUPERSTEP : PartialFunction[Any, Unit] = {
    case PrepareSuperStep(numVertices, numEdges) =>
      vertexProgramInstance match {
        case program : VertexProgram[I, V, E, M] =>
          program.load(config)
          val combinable: Option[Combinable[M]] = vertexProgramInstance match {
            case combinable : Combinable[M] => Some(combinable)
            case _ => None
          }
          if (combinable.isDefined) {
            program.messageSenders(
              (m, dst ) => {
                val index = partitioner.destination(dst)
                messagesCombinableSerializer.serialize(index, dst, m) { implicit byteBuffer =>
                  messageSender.send(id.get, index, byteBuffer)
                }
              },
              (v,m) => {
                v.edges.foreach {
                  case Edge(dst, _) =>
                    val index = partitioner.destination(dst)
                    messagesCombinableSerializer.serialize(index, dst, m) { implicit byteBuffer =>
                      messageSender.send(id.get, index, byteBuffer)
                    }
                }
              }
            )
          } else {
            program.messageSenders(
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
          program.totalNumVertices(numVertices)
          program.totalNumEdges(numEdges)
          vertexProgramInstance match {
            case aggregatable : Aggregatable => {
              aggregatable.initializeAggregators()
            }
            case _ =>
          }
        case _ =>
      }
      vertexProgramInstance match {
        case combinable: Combinable[M] =>
          messagesCombinableSerializer = new CombinableAsyncSerializer[I,M](0, workers.size, new Kryo(),
            combinable, vertexSerializer, messageSerializer, messageDeserializer)
        case _ =>
          messagesSerializer = new AsyncSerializer(0, workers.size, new Kryo())
      }
      messagesDeserializer = new AsyncDeserializer(0, workers.size, new Kryo())
      sender() ! true
    case RunSuperStep(superStep) =>
      Future{
        log.info(s"Running superstep $superStep")
//        log.info(s"$clazzI $clazzV $clazzE $clazzM")
        def addEdgeVertex : (I, I, E) => Unit = addEdge

        val v : Vertex[I,V,E] = new VertexImpl[I,V,E,M] {
          override def addEdge(dst: I, e: E): Unit = {
            addEdgeVertex(id, dst, e) // As of bufferProvider in ReusableIterable, the changes are immediately visible to new iterators
          }
        }

        val combinable: Option[Combinable[M]] = vertexProgramInstance match {
          case combinable : Combinable[M] => Some(combinable)
          case _ => None
        }

        val aggregatable : Option[Aggregatable] = vertexProgramInstance match {
          case aggregatable : Aggregatable => {
            Some(aggregatable)
          }
          case _ => None
        }

        allHalted = true
        vertices.foreach { vId =>
          val vMessages = messages(vId, clazzM)
          val vHalted = halted(vId)
          val hasMessage = vMessages.nonEmpty
          if (!vHalted || hasMessage) {
            val value : V = vertexValue(vId)
            val edgesIterable : Iterable[Edge[I,E]] = edges(vId)
            v.load(vId, value, edgesIterable)

            val hasHalted = vertexProgramInstance match {
              case program : VertexProgram[I,V,E,M] => program.run(v, vMessages, superStep)
              //                  case program : PerfectHashFormat[I] => program.run(v, superStep, new Mapper())
              case _ => true // TODO exception
            }
            setVertexValue(vId, v.value)
            setHalted(vId, hasHalted)

            if (!hasHalted) {
              allHalted = false
            }

            releaseEdgesIterable(edgesIterable)
          }
          releaseMessages(vMessages, clazzM)
        }

        if (combinable.isDefined) {
          messagesCombinableSerializer.sendNonEmptyByteBuffers{ case (byteBuffer : ByteBuffer, index : Int) =>
            messageSender.send(id.get, index, byteBuffer)
          }
        } else {
          messagesSerializer.sendNonEmptyByteBuffers { case (byteBuffer : ByteBuffer, index : Int) =>
            messageSender.send(id.get, index, byteBuffer)
          }
        }

        // trigger oncomplete
        vertexProgramInstance.onSuperStepComplete()

        aggregatable match {
          case Some(x) =>
            // send it to the master
            master() ! Aggregators(x.aggregators())
            x.aggregators().values.foreach(_.workerOnStepComplete())
            x.printAggregatedValues()
          case None =>
        }

        sendingComplete()
        superStepCompleteTrigger()
      }.recover{
        case t : Throwable => t.printStackTrace()
      }
    case byteString : ByteString =>
      handleSuperStepAkka(sender(), byteString)
    case SuperStepComplete() =>
      messagesOnSuperStepComplete()
      if (allHalted && emptyCurrentMessages) {
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

  def messagePairDeserializer(kryo : Kryo, input : Input) : (I, M) = {
    (kryo.readObject(input, clazzI),
      kryo.readObject(input, clazzM))
  }

  def messagePairSerializer(kryo : Kryo, output : Output, o : (I,M) ) = {
    kryo.writeObject(output, o._1)
    kryo.writeObject(output, o._2)
  }

  def messageSerializer(kryo : Kryo, output : Output, m : M) = {
    kryo.writeObject(output, m)
  }

  def messageDeserializer(kryo : Kryo, input : Input) : M = {
    kryo.readObject(input, clazzM)
  }

  private[this] def currentReceive : PartialFunction[Any, Unit] = _currentReceive
  private[this] def currentReceive_=(that : PartialFunction[Any, Unit]) = _currentReceive = that

  private[this] val POST_SUPERSTEP_OPERATION : PartialFunction[Any, Unit] = {
    case DoOutput(outputPath) =>
      Future {
        log.info(s"Writing output to ${outputPath}")
        jobSettings.outputDataLineWriter.newInstance().write(config, outputPath, (outputStream) => {

          val v : Vertex[I,V,E] = new VertexImpl[I,V,E,M] {
            override def addEdge(dst: I, e: E): Unit = {} // noop
          }
          vertices.foreach { vId =>
            v.load(vId, vertexValue(vId), edges(vId))
            programDefinition.outputWriter(v, outputStream)
          }
        })
        log.info(s"Done writing output ${outputPath}")
        master() ! DoneOutput()
      }.recover{
        case t : Throwable => t.printStackTrace()
      }
  }

  private[this] val BASE_OPERATION : PartialFunction[Any, Unit] = {
    case MasterAddress(address) =>
      masterActorRef = address
      sender() ! true
    case WorkerId(workerId) =>
      log.info(s"worker id: $workerId")
      val senderRef = sender()
      this.id = Option(workerId)
      messageReceiver = new MessageReceiverNetty
      messageReceiver.connect().foreach { success =>
        if (success) {
          senderRef ! AddressPair(self, NettyAddress(messageReceiver.host, messageReceiver.port))
          log.info(s"$workerId sent self! ?!?!")
        } else {
          log.error("Could not listen on ANY port")
        }
      }

    case WorkerMap(workers) =>
      log.info(s"workers: $workers")
      this.workers = workers
      this.workerPathsToIndex = this.workers.map(_.actorRef).zipWithIndex.toMap
      messageSender = new MessageSenderNetty(this)
      val senderRef = sender()
      FutureUtil.callbackOnAllComplete{ // TODO handle failure
      val destinationAddressPairs = workers.zipWithIndex.map{case (addressPair, index) => (index, (addressPair.nettyAddress.host, addressPair.nettyAddress.port))}
        messageSender.connectToAll(destinationAddressPairs)
      } {
        senderRef ! true
      }
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
          messageReceiver.setOnReceivedMessage(handleDataLoadingNetty)
          currentReceive = (BASE_OPERATION :: POST_SUPERSTEP_OPERATION :: Nil).reduceLeft(_ orElse _)
        case GlobalState.NONE =>
      }
      context.become(currentReceive, true)
      log.info(s"Set state to $newState")
      sender() ! true
    case Terminate() =>
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

  def vertexDeserializer(kryo : Kryo, input : Input) : I = {
    kryo.readObject(input, clazzI)
  }

  def vertexSerializer(kryo: Kryo, output : Output, v : I) : Unit = {
    kryo.writeObject(output, v)
  }

  def edgeDeserializer(kryo : Kryo, input : Input) : (I, I, E) = {
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

  def edgeSerializer(kryo : Kryo, output : Output, o : (I, I, E)) : Unit = {
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
