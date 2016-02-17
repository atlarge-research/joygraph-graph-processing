package nl.joygraph.core.actor

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.pool.{KryoFactory, KryoPool}
import com.typesafe.config.Config
import nl.joygraph.core.config.JobSettings
import nl.joygraph.core.format.Format
import nl.joygraph.core.message._
import nl.joygraph.core.reader.LineProvider
import nl.joygraph.core.util._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.reflect._

object WorkerState extends Enumeration {
  val NONE, LOAD_DATA = Value
}

class Worker[I : ClassTag,V : ClassTag,E : ClassTag,M : ClassTag](private[this] val config : Config
                     ) extends Actor with ActorLogging with MessageCounting {

  // TODO use different execution contexts at different places.
  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val clazzI : Class[I] = classTag[I].runtimeClass.asInstanceOf[Class[I]]
  private[this] val clazzV : Class[V] = classTag[V].runtimeClass.asInstanceOf[Class[V]]
  private[this] val clazzE : Class[E] = classTag[E].runtimeClass.asInstanceOf[Class[E]]
  private[this] val clazzM : Class[M] = classTag[M].runtimeClass.asInstanceOf[Class[M]]

  private[this] val pool = new KryoPool.Builder(new KryoFactory() {
    override def create(): Kryo = {
      val kryo = new Kryo()
      kryo
    }
  }).softReferences().build()

  private[this] var id : Option[Int] = None
  private[this] var workers : ArrayBuffer[String] = null
  private[this] val jobSettings = JobSettings(config)
  private[this] var verticesBufferNew : AsyncSerializer = null
  private[this] var edgeBufferNew : AsyncSerializer = null
  private[this] val halted = ArrayBuffer.empty[Boolean]
  private[this] val vEdges = TrieMap.empty[I, ConcurrentLinkedQueue[I]]
  private[this] val messages = TrieMap.empty[I, ConcurrentLinkedQueue[M]]
  private[this] var masterAddress : String = null

  protected[this] def master() = context.actorSelection(masterAddress)

  import WorkerState._
  var _state = NONE

  def state = _state
  def state_=(state : WorkerState.Value): Unit = {
    _state = state
  }

  def getCollection(vertex : I) : ConcurrentLinkedQueue[I] = {
    vEdges.getOrElseUpdate(vertex, new ConcurrentLinkedQueue[I])
//    if (!vEdges.contains(vertex)) {
//      synchronized {
//        if (!vEdges.contains(vertex)) {
//          val col = new ConcurrentLinkedQueue[I]()
//          vEdges.put(vertex, col)
//          col
//        } else {
//          vEdges(vertex)
//        }
//      }
//    } else {
//      vEdges(vertex)
//    }
  }

  def addVertex(vertex : I) : Unit = getCollection(vertex)

  def addEdge(edge : Array[I]): Unit = {
    val neighbours = getCollection(edge(0))
    neighbours.add(edge(1))
  }

  override def receive = {
    case MasterAddress(address) =>
      masterAddress = address
      sender() ! true
    case WorkerId(workerId) =>
      log.info(s"worker id: $workerId")
      this.id = Option(workerId)
    case WorkerMap(workers) =>
      log.info(s"workers: $workers")
      this.workers = workers
      // TODO move to a better place
      this.edgeBufferNew = new AsyncSerializer(0, workers.length, new Kryo())
      this.verticesBufferNew = new AsyncSerializer(1, workers.length, new Kryo())
      sender() ! true
    case LoadData(path, start, length) =>
      Future {
        val lineProvider : LineProvider = jobSettings.inputDataLineProvider.newInstance()
        lineProvider.path = jobSettings.dataPath
        lineProvider.start = start
        lineProvider.length = length
        lineProvider.initialize(config)

        val parser: Format[I] = jobSettings.inputFormatClass.newInstance().asInstanceOf[Format[I]]

        lineProvider.foreach{ l =>
          val edge: Array[I] = parser.parse(l)
          val index : Int = edge(0).hashCode() % workers.length
          val index2 : Int = edge(1).hashCode() % workers.length
          if (index == id.get) {
            addEdge(edge)
          } else {
            edgeSerializer(edge, index)
          }
          if (index2 == id.get) {
            addVertex(edge(1))
          } else {
            vertexSerializer(edge(1), index2)
          }
        }
        workers.zipWithIndex.foreach{
          case (actorAddress, index) =>
            Iterable(verticesBufferNew.buffer(index), edgeBufferNew.buffer(index)).foreach(os => sendByteArray(context.actorSelection(actorAddress), os.toByteArray))
        }
        sendingComplete()
      }
    case PrepareLoadData() =>
      state = LOAD_DATA
      println(s"$id: prepare load data!~ $state")
      sender() ! true
    case byteArray : Array[Byte] =>
      state match {
        case LOAD_DATA =>
          val senderRef = sender()
          Future {
            val is = new ObjectByteArrayInputStream(byteArray)
            is.msgType match {
              case 0 => // edge
                val input = new Input(is)
                val edges = new Iterator[Array[I]] {
                  var numObjects = 0

                  override def hasNext: Boolean = numObjects < is.counter

                  override def next(): Array[I] = {
                    numObjects += 1
                    edgeDeserializer(input)
                  }
                }
                edges.foreach(addEdge)
              case 1 => // vertex
                val input = new Input(is)
                val vertices = new Iterator[I] {
                  var numObjects = 0

                  override def hasNext: Boolean = numObjects < is.counter

                  override def next(): I = {
                    numObjects += 1
                    vertexDeserializer(input)
                  }
                }
                vertices.foreach(addVertex)
            }

            senderRef ! Received()
          }
        case NONE =>
          println(s"$id: $state")
      }
    case Received() =>
      receivedByteArrayAck()
    case AllLoadingComplete() =>
      state match {
        case LOAD_DATA =>
          master() ! LoadingComplete(id.get, vEdges.keys.size, vEdges.values.map(_.size()).sum)
        case NONE =>
      }
  }

  def receivedByteArrayAck(): Unit = {
    incrementReceived()
    if (doneAllSentReceived) {
      master() ! AllLoadingComplete()
      resetSentReceived()
    }

  }

  def sendByteArray(dst : ActorSelection, bytes: Array[Byte]): Unit = {
    dst ! bytes
    incrementSent()
  }

  def edgeDeserializer[U >: I : ClassTag](input : Input) : Array[U] = {
    val arr = new Array[U](2)
    val kryo = pool.borrow()
    arr(0) = kryo.readObject(input, clazzI)
    arr(1) = kryo.readObject(input, clazzI)
    pool.release(kryo)
    arr
  }

  def vertexDeserializer(input : Input) : I = {
    val kryo = pool.borrow()
    val res = kryo.readObject(input, clazzI)
    pool.release(kryo)
    res
  }

  def vertexSerializer(v : I, index : Int) : Unit = {
    verticesBufferNew.serialize(v, index) { implicit os =>
      sendByteArray(context.actorSelection(workers(index)), os.toByteArray)
      os.reset()
    }
  }

  def edgeSerializer(v : Array[I], index : Int) : Unit = {
    edgeBufferNew.serialize(v.toIterable, index) { implicit os =>
      sendByteArray(context.actorSelection(workers(index)), os.toByteArray)
      os.reset()
    }
  }
}
