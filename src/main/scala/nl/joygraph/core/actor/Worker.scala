package nl.joygraph.core.actor

import java.nio.ByteBuffer

import akka.actor._
import akka.util.ByteString
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.config.Config
import nl.joygraph.core.actor.communication.impl.netty.{MessageReceiverNetty, MessageSenderNetty}
import nl.joygraph.core.actor.messaging.MessageStore
import nl.joygraph.core.actor.messaging.impl.TrieMapMessageStore
import nl.joygraph.core.actor.messaging.impl.serialized.TrieMapSerializedMessageStore
import nl.joygraph.core.actor.state.GlobalState
import nl.joygraph.core.actor.vertices.VerticesStore
import nl.joygraph.core.actor.vertices.impl.TrieMapVerticesStore
import nl.joygraph.core.actor.vertices.impl.serialized.TrieMapSerializedVerticesStore
import nl.joygraph.core.config.JobSettings
import nl.joygraph.core.message._
import nl.joygraph.core.message.superstep._
import nl.joygraph.core.partitioning.VertexPartitioner
import nl.joygraph.core.program._
import nl.joygraph.core.reader.LineProvider
import nl.joygraph.core.util._
import nl.joygraph.core.util.io.streams.bytebuffer.ObjectByteBufferInputStream
import nl.joygraph.core.util.serde.{AsyncDeserializer, AsyncSerializer}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.reflect._

object Worker{
  def workerWithTrieMapMessageStore[I : ClassTag,V : ClassTag,E : ClassTag,M : ClassTag]
  (config: Config,
   parser: (String) => (I, I, E),
   clazz : Class[_ <: VertexProgramLike[I,V,E,M]],
   partitioner : VertexPartitioner
  ): () => Worker[I,V,E,M] = () => {
    new Worker[I,V,E,M](config, parser, clazz, partitioner) with TrieMapMessageStore[I,M] with TrieMapVerticesStore[I,V,E]
  }

  def workerWithSerializedTrieMapMessageStore[I : ClassTag,V : ClassTag,E : ClassTag,M : ClassTag]
  (config: Config,
   parser: (String) => (I, I, E),
   clazz : Class[_ <: VertexProgramLike[I,V,E,M]],
   partitioner : VertexPartitioner
  ): () => Worker[I,V,E,M] = () => {
    new Worker[I,V,E,M](config, parser, clazz, partitioner) with TrieMapSerializedMessageStore[I,M] with TrieMapSerializedVerticesStore[I,V,E]
//    new Worker[I,V,E,M](config, parser, clazz, partitioner) with TrieMapMessageStore[I,M] with TrieMapSerializedVerticesStore[I,V,E]
  }
}

abstract class Worker[I : ClassTag,V : ClassTag,E : ClassTag,M : ClassTag]
(private[this] val config : Config,
 parser: (String) => (I, I, E),
 clazz : Class[_ <: VertexProgramLike[I,V,E,M]],
 protected[this] var partitioner : VertexPartitioner)
  extends Actor with ActorLogging with MessageCounting with MessageStore[I,M] with VerticesStore[I,V,E] {

  // TODO use different execution contexts at different places.
  import scala.concurrent.ExecutionContext.Implicits.global

  protected[this] val clazzI : Class[I] = classTag[I].runtimeClass.asInstanceOf[Class[I]]
  protected[this] val clazzV : Class[V] = classTag[V].runtimeClass.asInstanceOf[Class[V]]
  protected[this] val clazzE : Class[E] = classTag[E].runtimeClass.asInstanceOf[Class[E]]
  protected[this] val clazzM : Class[M] = classTag[M].runtimeClass.asInstanceOf[Class[M]]

  private[this] var id : Option[Int] = None
  private[this] var workers : ArrayBuffer[AddressPair] = null
  private[this] var workerPathsToIndex : Map[ActorRef, Int] = null

  private[this] val jobSettings = JobSettings(config)
  private[this] var verticesBufferNew : AsyncSerializer[I] = null
  private[this] var edgeBufferNew : AsyncSerializer[(I,I,E)] = null
  private[this] var verticesDeserializer : AsyncDeserializer[I] = null
  private[this] var edgesDeserializer : AsyncDeserializer[(I,I,E)] = null

  private[this] var masterActorRef : ActorRef = null
  private[this] val vertexProgramInstance = clazz.newInstance()

  private[this] var messageSender : MessageSenderNetty = _
  private[this] var messageReceiver : MessageReceiverNetty = _

  private[this] var messagesSerializer : AsyncSerializer[(I, M)] = null
  private[this] var messagesDeserializer : AsyncDeserializer[(I, M)] = null
  private[this] var allHalted = true


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
        messagesDeserializer.deserialize(is, index, messageDeserializer){ implicit dstMPairs =>
          dstMPairs.foreach(_handleMessage(index, _))
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
      this.edgesDeserializer = new AsyncDeserializer[(I,I,E)](0, workers.length, new Kryo())
      this.verticesDeserializer = new AsyncDeserializer[I](1, workers.length, new Kryo())
      sender() ! true
    case LoadData(path, start, length) =>
      Future {
        val lineProvider : LineProvider = jobSettings.inputDataLineProvider.newInstance()
        lineProvider.path = jobSettings.dataPath
        lineProvider.start = start
        lineProvider.length = length
        lineProvider.initialize(config)

        lineProvider.foreach{ l =>
          val (src, dst, value) = parser(l)
          val index : Int = partitioner.destination(src)
          val index2 : Int = partitioner.destination(dst)
          if (index == id.get) {
            addEdge(src, dst, value)
          } else {
            edgeBufferNew.serialize(index, (src, dst, value), edgeSerializer) { implicit byteBuffer =>

              println(s"- sending to $index, size: ${byteBuffer.remaining()}")
              messageSender.send(id.get, index, byteBuffer)
            }
          }
          if (index2 == id.get) {
            addVertex(dst)
          } else {
            verticesBufferNew.serialize(index2, dst, vertexSerializer) { implicit byteBuffer =>
              println(s"- sending to $index2, size: ${byteBuffer.remaining()}")

              messageSender.send(id.get, index2, byteBuffer)
            }
          }
        }

        verticesBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, index : Int) =>
          println(s"final sending to $index, size: ${byteBuffer.remaining()}")
          messageSender.send(id.get, index, byteBuffer)
        })

        edgeBufferNew.sendNonEmptyByteBuffers({ case (byteBuffer : ByteBuffer, index : Int) =>
          println(s"final sending to $index, size: ${byteBuffer.remaining()}")
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
    case PrepareSuperStep() =>
      vertexProgramInstance match {
        case program : VertexProgram[I, V, E, M] =>
          program.load(config)
        case _ =>
      }
      messagesSerializer = new AsyncSerializer[(I, M)](0, workers.size, new Kryo())
      messagesDeserializer = new AsyncDeserializer[(I, M)](0, workers.size, new Kryo())
      sender() ! true
    case RunSuperStep(superStep) =>
      Future{
        log.info(s"Running superstep $superStep")
          val v : Vertex[I,V,E,M] = new VertexImpl[I,V,E,M]
          allHalted = true
          vertices.foreach { vId =>
              val vMessages = messages(vId)
              val vHalted = halted(vId)
              val hasMessage = vMessages.nonEmpty
              if (!vHalted || hasMessage) {
                val value : V = vertexValue(vId)
                val edgesIterable : Iterable[Edge[I,E]] = edges(vId)
                val messageOut : scala.collection.mutable.MultiMap[I,M] = new scala.collection.mutable.OpenHashMap[I, scala.collection.mutable.Set[M]] with scala.collection.mutable.MultiMap[I, M]
                val allMessages = ArrayBuffer.empty[M]
                v.load(vId, value, edgesIterable, messageOut, allMessages)
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

                // send messages
                messageOut.foreach {
                  case (dst, m) =>
                    val index = partitioner.destination(dst)
                    m.foreach {
                      x => messagesSerializer.serialize(index, (dst, x), messageSerializer) { implicit byteBuffer =>
                        messageSender.send(id.get, index, byteBuffer)
                      }
                    }
                }
                allMessages.foreach {
                  m =>
                    edgesIterable.foreach {
                      case Edge(dst, _) =>
                        val index = partitioner.destination(dst)
                        messagesSerializer.serialize(index, (dst, m), messageSerializer) { implicit byteBuffer =>
                          messageSender.send(id.get, index, byteBuffer)
                        }
                    }
                }
                releaseEdgesIterable(edgesIterable)
              }
            releaseMessages(vMessages)
          }

        messagesSerializer.sendNonEmptyByteBuffers { case (byteBuffer : ByteBuffer, index : Int) =>
          messageSender.send(id.get, index, byteBuffer)
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

  def messageDeserializer(kryo : Kryo, input : Input) : (I, M) = {
    (kryo.readObject(input, clazzI),
    kryo.readObject(input, clazzM))
  }

  def messageSerializer(kryo : Kryo, output : Output, o : (I,M) ) = {
    kryo.writeObject(output, o._1)
    kryo.writeObject(output, o._2)
  }

  private[this] def currentReceive : PartialFunction[Any, Unit] = _currentReceive
  private[this] def currentReceive_=(that : PartialFunction[Any, Unit]) = _currentReceive = that

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
        case GlobalState.NONE =>
      }
      context.become(currentReceive, true)
      log.info(s"Set state to $newState")
      sender() ! true
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
