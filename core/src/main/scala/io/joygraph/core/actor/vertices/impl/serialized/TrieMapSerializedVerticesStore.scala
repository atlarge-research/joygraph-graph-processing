package io.joygraph.core.actor.vertices.impl.serialized

import java.util.concurrent.atomic.AtomicLong

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import com.esotericsoftware.kryo.pool.{KryoFactory, KryoPool}
import io.joygraph.core.actor.vertices.VerticesStore
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program.{Edge, NullClass}
import io.joygraph.core.util.collection.ReusableIterable
import io.joygraph.core.util.{DirectByteBufferGrowingOutputStream, KryoSerialization, SimplePool}

import scala.collection.concurrent.TrieMap

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
  private[this] val kryoPool = new KryoPool.Builder(new KryoFactory {
    override def create(): Kryo = new Kryo()
  }).build()


  private[this] def getStream(vertex : I) = _vEdges.getOrElseUpdate(vertex, new DirectByteBufferGrowingOutputStream(0))

  override protected[this] def addVertex(vertex: I): Unit = getStream(vertex)

  override protected[this] def addEdge(src: I, dst: I, value: E): Unit = {
    numEdgesCounter.incrementAndGet()
    val index = partitioner.destination(src)
    val kryoInstance = kryo(index)
    kryoInstance.synchronized {
      val kryoOutputInstance = kryoOutput(index)
      val os = getStream(src)
      kryoOutputInstance.setOutputStream(os)
      kryoInstance.writeObject(kryoOutputInstance, dst)
      if (!isNullClass) {
        kryoInstance.writeObject(kryoOutputInstance, value)
      }
      kryoOutputInstance.flush()
      os.trim()
    }
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
          val bb = os.getBuf
          bb.flip()
          reusableIterablePool.borrow()
            .kryo(kryoPool.borrow())
            .buffer(bb)
        }
      case None =>
        NO_EDGES
    }
  }

  protected[this] def releaseEdgesIterable(edgesIterable : Iterable[Edge[I,E]]) = {
    edgesIterable match {
      case reusableIterable : ReusableIterable[Edge[I,E]] =>
        kryoPool.release(reusableIterable.kryo)
        reusableIterablePool.release(reusableIterable)
      case _ => // noop
    }
  }

  protected[this] def vertexValue(vId : I) : V = _vValues.getOrElse(vId, null.asInstanceOf[V])

  protected[this] def setVertexValue(vId : I, v : V) = _vValues(vId) = v
}
