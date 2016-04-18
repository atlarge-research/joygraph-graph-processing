package io.joygraph.programs

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{DefaultSerializer, Kryo, KryoSerializable}
import com.typesafe.config.Config
import io.joygraph.core.program.{NewVertexProgram, NullClass, SuperStepFunction, Vertex}
import io.joygraph.core.util.LazySize

import scala.collection.mutable

class ULCC extends NewVertexProgram[Long, Double, NullClass] {
  override def load(conf: Config): Unit = {}

  // TODO neighbours creates a lot of garbage, maybe replace with something else
  private[this] val neighbours = mutable.HashSet.empty[Long]
  private[this] val inquiry = Inquiry(null.asInstanceOf[Long], null.asInstanceOf[Array[Long]])

  override def run(): PartialFunction[Int, SuperStepFunction[Long, Double, NullClass, _, _]] = {
    case 0 =>
      new SuperStepFunction(this, classOf[Long], classOf[Inquiry]) {
        override def func: (Vertex[Long, Double, NullClass], Iterable[Long]) => Boolean = (v, m) => {
          neighbours.clear()
          v.edges.foreach(x => neighbours += x.dst)
          if (neighbours.size > 1) {
            val nArray: Array[Long] = neighbours.toArray
            inquiry.src = v.id
            inquiry.edges = nArray
            neighbours.foreach(dst => send(inquiry, dst))
          }
          v.value = neighbours.size
          false
        }
      }
    case 1 =>
      new SuperStepFunction(this, classOf[Inquiry], classOf[Int]) {
        override def func: (Vertex[Long, Double, NullClass], Iterable[Inquiry]) => Boolean = (v, m) => {
          neighbours.clear()
          v.edges.foreach(x => neighbours += x.dst)
          m.foreach {
            case Inquiry(dst, edgeList) =>
              var matchCount = 0
              edgeList.foreach(edge => {
                if (neighbours.contains(edge)) {
                  matchCount += 1
                }
              })
              send(matchCount, dst)
          }
          false
        }
      }
    case 2 =>
      new SuperStepFunction(this, classOf[Int], classOf[NullClass]) {
        override def func: (Vertex[Long, Double, NullClass], Iterable[Int]) => Boolean = (v, m) => {
          if (LazySize.sizeSmallerThan(m, 2)) {
            v.value = 0.0
          } else {
            val numMatches: Long = m.sum
            val numNeighbours = v.value
            v.value = numMatches / numNeighbours / (numNeighbours - 1)
          }
          true
        }
      }
  }
}

class DLCC extends NewVertexProgram[Long, Double, NullClass] {
  override def load(conf: Config): Unit = {}

  // TODO neighbours creates a lot of garbage, maybe replace with something else
  private[this] val neighbours = mutable.HashSet.empty[Long]
  private[this] val inquiry = Inquiry(null.asInstanceOf[Long], null.asInstanceOf[Array[Long]])
  override def run(): PartialFunction[Int, SuperStepFunction[Long, Double, NullClass, _, _]] = {
    case 0 =>
      new SuperStepFunction(this, classOf[NullClass], classOf[Long]) {
        override def func: (Vertex[Long, Double, NullClass], Iterable[NullClass]) => Boolean = (v, m) => {
          sendAll(v, v.id)
          false
        }
      }
    case 1 =>
      new SuperStepFunction(this, classOf[Long], classOf[Inquiry]) {
        override def func: (Vertex[Long, Double, NullClass], Iterable[Long]) => Boolean = (v, m) => {
          neighbours.clear()
          neighbours ++= m
          v.edges.foreach(x => neighbours += x.dst)
          if (neighbours.size > 1) {
            val nArray: Array[Long] = neighbours.toArray
            inquiry.src = v.id
            inquiry.edges = nArray
            neighbours.foreach(dst => send(inquiry, dst))
          }
          v.value = neighbours.size
          false
        }
      }
    case 2 =>
      new SuperStepFunction(this, classOf[Inquiry], classOf[Int]) {
        override def func: (Vertex[Long, Double, NullClass], Iterable[Inquiry]) => Boolean = (v, m) => {
          neighbours.clear()
          v.edges.foreach(x => neighbours += x.dst)
          m.foreach {
            case Inquiry(dst, edgeList) =>
              var matchCount = 0
              edgeList.foreach(edge => {
                if (neighbours.contains(edge)) {
                  matchCount += 1
                }
              })
              send(matchCount, dst)
          }
          false
        }
      }
    case 3 =>
      new SuperStepFunction(this, classOf[Int], classOf[NullClass]) {
        override def func: (Vertex[Long, Double, NullClass], Iterable[Int]) => Boolean = (v, m) => {
          if (LazySize.sizeSmallerThan(m, 2)) {
            v.value = 0.0
          } else {
            val numMatches: Long = m.sum
            val numNeighbours = v.value
            v.value = numMatches / numNeighbours / (numNeighbours - 1)
          }
          true
        }
      }
  }
}

object Inquiry {
  def apply(src : Long, edges : Array[Long]) : Inquiry = new Inquiry(src, edges)
  def unapply(o : Inquiry) : Option[(Long, Array[Long])] = Some(o.src, o.edges)
}

class Inquiry extends KryoSerializable {
  var src : Long = _
  var edges : Array[Long] = _
  def this(src : Long, edges : Array[Long]) = {
    this()
    this.src = src
    this.edges = edges
  }

  override def write(kryo: Kryo, output: Output): Unit =  {
    output.writeLong(src)
    output.writeInt(edges.length)
    edges.foreach(output.writeLong)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    src = input.readLong()
    edges = new Array(input.readInt())
    var i = 0
    while (i < edges.length) {
      edges(i) = input.readLong()
      i += 1
    }
  }
}

