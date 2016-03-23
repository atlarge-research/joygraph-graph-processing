package io.joygraph.core.actor.vertices.impl.serialized

import java.util.concurrent.atomic.AtomicLong

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import com.esotericsoftware.kryo.pool.{KryoFactory, KryoPool}
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program.{Edge, NullClass}
import io.joygraph.core.util.buffers.KryoOutput
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.{DirectByteBufferGrowingOutputStream, KryoSerialization, SimplePool}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer

trait TrieMapSerializedVerticesStore[I,V,E] extends VerticesStore[I,V,E] with KryoSerialization {

  protected[this] val clazzI : Class[I]
  protected[this] val clazzE : Class[E]
  protected[this] var partitioner : VertexPartitioner

  private[this] val _halted = TrieMap.empty[I, Boolean]
  private[this] val _vEdges = TrieMap.empty[I, DirectByteBufferGrowingOutputStream]
  private[this] val _vValues = TrieMap.empty[I, V]
  private[this] val numEdgesCounter = new AtomicLong(0)
  private[this] val isNullClass = clazzE == classOf[NullClass]
  private[this] val NO_EDGES = Iterable.empty[Edge[I,E]]
  private[this] val reusableIterablePool = new SimplePool[ReusableIterable[Edge[I,E]]]({
    new ReusableIterable[Edge[I,E]] {
      private[this] val reusableEdge : Edge[I,E] = Edge[I,E](null.asInstanceOf[I], null.asInstanceOf[E])
      override protected[this] def deserializeObject(): Edge[I, E] = {
        reusableEdge.dst = _kryo.readObject(_input, clazzI)
        if (!isNullClass) {
          reusableEdge.e = _kryo.readObject(_input, clazzE)
        }
        reusableEdge
      }
    }.input(new ByteBufferInput(maxMessageSize))
  })

  private[this] val reusableMutableIterablePool = new SimplePool[MutableReusableIterable[I, Edge[I,E]]]( {
    val mRIterable = new MutableReusableIterable[I, Edge[I, E]] {
      override protected[this] def deserializeObject(): Edge[I, E] = {
        if (!isNullClass) {
          Edge(_kryo.readObject(_input, clazzI), _kryo.readObject(_input, clazzE))
        } else {
          Edge(_kryo.readObject(_input, clazzI), null.asInstanceOf[E])
        }
      }

      private[this] val readOnlyIterable = new Iterable[Edge[I,E]] {
        override def iterator: Iterator[Edge[I, E]] = new Iterator[Edge[I, E]] {
          private[this] val readOnlyEdge: Edge[I, E] = new Edge[I, E](null.asInstanceOf[I], null.asInstanceOf[E])
          private[this] val currentBuffer = mutatedBuffer

          override def hasNext: Boolean = mutatedBuffer.hasNext

          override def next(): Edge[I, E] = {
            val nextEdge = mutatedBuffer.next()
            // TODO the reference is passed so one could still change dst or e as sideeffect if it's not a primitive
            readOnlyEdge.dst = nextEdge.dst
            readOnlyEdge.e = nextEdge.e
            readOnlyEdge
          }
        }
      }

      override def readOnly: Iterable[Edge[I, E]] = {
        readOnlyIterable
      }
    }
    mRIterable.input(new ByteBufferInput(maxMessageSize))
    mRIterable
  })

  // TODO inject factory from worker
  private[this] val kryoPool = new KryoPool.Builder(new KryoFactory {
    override def create(): Kryo = new Kryo()
  }).build()


  private[this] def getStream(vertex : I) = _vEdges.getOrElseUpdate(vertex, new DirectByteBufferGrowingOutputStream(0))

  override protected[this] def addVertex(vertex: I): Unit = getStream(vertex)

  override protected[this] def addEdge(src: I, dst: I, value: E): Unit = {
    numEdgesCounter.incrementAndGet()
    val index = partitioner.destination(src)
    implicit val kryoInstance = kryo(index)
    kryoInstance.synchronized {
      implicit val kryoOutputInstance = kryoOutput(index)
      implicit val os = getStream(src)
      serializeEdge(dst, value)
    }
  }

  private[this] def serializeEdge(dst : I, value : E)
                                 (implicit kryo : Kryo, kryoOutput : KryoOutput, os : DirectByteBufferGrowingOutputStream): Unit = {

    kryoOutput.setOutputStream(os)
    kryo.writeObject(kryoOutput, dst)
    if (!isNullClass) {
      kryo.writeObject(kryoOutput, value)
    }
    kryoOutput.flush()
    os.trim()
  }


  override protected[this] def numEdges: Int = numEdgesCounter.intValue()

  override protected[this] def vertices: Iterable[I] = new Iterable[I] {
    override def iterator: Iterator[I] = _vEdges.keysIterator
  }

  override protected[this] def numVertices: Int = _vEdges.size

  override protected[this] def halted(vId : I) : Boolean = _halted.getOrElse(vId, false)

  override protected[this] def setHalted(vId : I, halted : Boolean) =
    if (halted) {
      _halted(vId) = true
    } else {
      _halted.remove(vId)
    }

  override protected[this] def edges(vId: I): Iterable[Edge[I, E]] = {
    _vEdges.get(vId) match {
      case Some(os) =>
        if (os.isEmpty) {
          NO_EDGES
        } else {
          reusableIterablePool.borrow()
            .kryo(kryoPool.borrow())
            .bufferProvider(() => os.getBuf)
        }
      case None =>
        NO_EDGES
    }
  }

  override protected[this] def mutableEdges(vId : I) : Iterable[Edge[I,E]] = {
    _vEdges.get(vId) match {
      case Some(os) =>
        if (os.isEmpty) {
          NO_EDGES
        } else {
          reusableMutableIterablePool.borrow()
            .vId(vId)
            .kryo(kryoPool.borrow())
            .bufferProvider(() => os.getBuf)
        }
      case None =>
        NO_EDGES
    }
  }

  protected[this] def releaseEdgesIterable(edgesIterable : Iterable[Edge[I,E]]) = {
    edgesIterable match {
      case mutableIterable : MutableReusableIterable[I,Edge[I,E] @unchecked] =>
        val src = mutableIterable.vId
        val index = partitioner.destination(src)
        implicit val kryoInstance = kryo(index)
        kryoInstance.synchronized {
          implicit val kryoOutputInstance = kryoOutput(index)
          implicit val os = getStream(src)
          if (mutableIterable.hasBeenUsed) {
            os.clear()
            // propagate changes to edges
            mutableIterable.mutatedBuffer.foreach{
              case Edge(dst, value) => serializeEdge(dst, value)
            }
            os.trim()
          }
        }

        // release resources
        kryoPool.release(mutableIterable.kryo)
        mutableIterable.reset()
        reusableMutableIterablePool.release(mutableIterable)
      case reusableIterable : ReusableIterable[Edge[I,E]] =>
        kryoPool.release(reusableIterable.kryo)
        reusableIterablePool.release(reusableIterable)
      case _ => // noop
    }
  }

  protected[this] def vertexValue(vId : I) : V = _vValues.getOrElse(vId, null.asInstanceOf[V])

  protected[this] def setVertexValue(vId : I, v : V) = _vValues(vId) = v
}

abstract class MutableReusableIterable[I, +T] extends ReusableIterable[T] {

  // better solution would be to alter the DirectByteBufferGrowingOutputStream in realtime
  // however the size of each object is irregular
  // and would require shifting of byte arrays, which may be more CPU intensive.
  private[this] var firstTime = true
  private[this] val buffer = ArrayBuffer.empty[T]
  private[this] var originalBufferLimit : Int = _
  private[this] var _vId : I = _

  def vId : I = _vId
  def vId(vId : I) : MutableReusableIterable[I,T] = {
    _vId = vId
    this
  }

  def hasBeenUsed = !firstTime

  def readOnly : Iterable[T]
  def mutatedBuffer : Iterator[T] = buffer.iterator

  override def iterator: Iterator[T] = {
    if (firstTime) {
      buffer.clear()
      buffer ++= super.iterator
      originalBufferLimit = _input.getByteBuffer.limit()
      firstTime = false
    }

    val currentBuffer = _bufferProvider()
    currentBuffer.flip()
    val currentBufferLimit = currentBuffer.limit()
    if (currentBufferLimit > originalBufferLimit) {
      // edges have been added
      // add new edges to buffer
      currentBuffer.position(originalBufferLimit)
      _input.setBuffer(currentBuffer)
      originalBufferLimit = currentBufferLimit
      _iterator.foreach(buffer += _)
    } else if(currentBufferLimit < originalBufferLimit) {
      // edges have been removed
      throw new UnsupportedOperationException("edges have been removed")
    } else {
      // no change
    }
    buffer.iterator
  }

  def reset() = firstTime = true
}