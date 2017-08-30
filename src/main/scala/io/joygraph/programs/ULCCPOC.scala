package io.joygraph.programs

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.typesafe.config.Config
import io.joygraph.core.program._
import io.joygraph.core.util.LazySize

import scala.collection.mutable

class DLCCPOC extends NewVertexProgram[Long, Double, Unit] {
  override def load(conf: Config): Unit = {
    // noop
  }

  private[this] val neighbours = mutable.HashSet.empty[Long]
  private[this] val inquiry = Inquiry(null.asInstanceOf[Long], null.asInstanceOf[Array[Long]])

  override def run(): PartialFunction[Int, SuperStepFunction[Long, Double, Unit]] = {
    case 0 =>
      new PregelSuperStepFunction(this, classOf[Unit], classOf[Long]) {
        override def func: (Vertex[Long, Double, Unit], Iterable[Unit]) => Boolean = (v, m) => {
          sendAll(v, v.id)
          false
        }
      }
    case 1 =>
      new PregelSuperStepFunction(this, classOf[Long], classOf[Unit]) with RequestResponse[Long, Double, Unit, Inquiry, Int] {

        override val reqClazz: Class[Inquiry] = classOf[Inquiry]
        override val resClazz: Class[Int] = classOf[Int]

        override def func: (Vertex[Long, Double, Unit], Iterable[Long]) => Boolean = (v, m) => {
          neighbours.clear()
          neighbours ++= m
          v.edges.foreach(x => neighbours += x.dst)
          if (neighbours.size > 1) {
            val nArray: Array[Long] = neighbours.toArray
            inquiry.src = v.id
            inquiry.edges = nArray
            var matchCount = 0
            neighbours.foreach { dst =>
              val matches = request(inquiry, dst)
              matchCount += matches
              matches
            }
            val numNeighbours : Double = neighbours.size
            val lcc = matchCount / numNeighbours / (numNeighbours - 1)
            v.value = lcc
          } else {
            v.value = 0.0
          }
          true
        }

        override def response(v : Vertex[Long, Double, Unit], r: Inquiry): Int = {
          if (v.edges.isEmpty) {
            0
          } else {
            val neighbours =  mutable.HashSet.empty[Long]
            v.edges.foreach(x => neighbours += x.dst)
            val matches = r.edges.count(neighbours.contains)
            matches
          }
        }
      }
  }
}

class DLCCPOC2 extends NewVertexProgram[Long, ValuePOC2, Unit] {
  private[this] val neighbours = mutable.HashSet.empty[Long]
  private[this] val inquiry = new InquiryPOC2(null.asInstanceOf[Long], null.asInstanceOf[Array[Long]])
  private[this] val nullRef = null.asInstanceOf[Unit]
  private[this] val emptyIterable = Iterable.empty[(Long, InquiryPOC2)]

  override def load(conf: Config): Unit = {
    // noop
  }

  override def run(): PartialFunction[Int, SuperStepFunction[Long, ValuePOC2, Unit]] = {
    case 0 =>
      new PregelSuperStepFunction(this, classOf[Unit], classOf[Long]) {
        override def func: (Vertex[Long, ValuePOC2, Unit], Iterable[Unit]) => Boolean = (v, m) => {
          // create neighbours hashset
          val neighbours =  mutable.HashSet.empty[Long]
          v.edges.foreach(x => neighbours += x.dst)
          v.value = new ValuePOC2(0.0, neighbours.toSet)
          sendAll(v, v.id)
          false
        }
      }
    case 1 =>
      new QueryAnswerProcessSuperStepFunction(this, classOf[InquiryPOC2], classOf[Int], classOf[Long]) {
        override def query(v: Vertex[Long, ValuePOC2, Unit], messages : Iterable[Long]): Iterable[(Long, InquiryPOC2)] = {
          neighbours.clear()
          v.edges.foreach(x => neighbours += x.dst)
          neighbours ++= messages
          if (neighbours.size > 1) {
            inquiry.src = v.id
            inquiry.edges = neighbours.toArray

            v.value.lcc = neighbours.size

            new Iterable[(Long, InquiryPOC2)] {
              override def iterator: Iterator[(Long, InquiryPOC2)] = new Iterator[(Long, InquiryPOC2)] {
                val nIt =  neighbours.iterator

                override def hasNext: Boolean = nIt.hasNext

                override def next(): (Long, InquiryPOC2) = (nIt.next(), inquiry)
              }
            }
          } else {
            emptyIterable
          }
        }

        override def answer(v : Vertex[Long, ValuePOC2, Unit], query: InquiryPOC2): Int = {
          if (v.edges.isEmpty) {
            0
          } else {
            val matches = query.edges.count(v.value.neighboursSet.contains)
            matches
          }
        }

        override def process(v: Vertex[Long, ValuePOC2, Unit], data: Iterable[Int]): Boolean = {
          if (data.isEmpty) {
            v.value.lcc = 0.0
          } else {
            data.sum match {
              case 0 =>
                v.value.lcc = 0
              case matches =>
                val numNeighbours : Double = v.value.lcc
                val lcc = matches / numNeighbours / (numNeighbours - 1)
                v.value.lcc = lcc
            }
          }
          true
        }

      }
  }
}

class ULCCPOC extends NewVertexProgram[Long, Double, Unit] {
  override def load(conf: Config): Unit = {
    // noop
  }

  private[this] val neighbours = mutable.HashSet.empty[Long]
  private[this] val inquiry = Inquiry(null.asInstanceOf[Long], null.asInstanceOf[Array[Long]])

  override def run(): PartialFunction[Int, SuperStepFunction[Long, Double, Unit]] = {
    case 0 =>
      new PregelSuperStepFunction(this, classOf[Unit], classOf[Unit]) with RequestResponse[Long, Double, Unit, Inquiry, Int] {

        override val reqClazz: Class[Inquiry] = classOf[Inquiry]
        override val resClazz: Class[Int] = classOf[Int]

        override def func: (Vertex[Long, Double, Unit], Iterable[Unit]) => Boolean = (v, m) => {
          neighbours.clear()
          v.edges.foreach(x => neighbours += x.dst)
          if (neighbours.size > 1) {
            val nArray: Array[Long] = neighbours.toArray
            inquiry.src = v.id
            inquiry.edges = nArray
            var matchCount = 0
            neighbours.foreach { dst =>
              val matches = request(inquiry, dst)
              matchCount += matches
              matches
            }
            val numNeighbours : Double = neighbours.size
            val lcc = matchCount / numNeighbours / (numNeighbours - 1)
            v.value = lcc
          } else {
            v.value = 0.0
          }
          true
        }

        override def response(v : Vertex[Long, Double, Unit], r: Inquiry): Int = {
          if (v.edges.isEmpty) {
            0
          } else {
            val neighbours =  mutable.HashSet.empty[Long]
            v.edges.foreach(x => neighbours += x.dst)
            val matches = r.edges.count(neighbours.contains)
            matches
          }
        }
      }
  }
}

class ULCCPOC2 extends NewVertexProgram[Long, ValuePOC2, Unit] {
  private[this] val neighbours = mutable.HashSet.empty[Long]
  private[this] val inquiry = new InquiryPOC2(null.asInstanceOf[Long], null.asInstanceOf[Array[Long]])
  private[this] val emptyIterable = Iterable.empty[(Long, InquiryPOC2)]

  override def load(conf: Config): Unit = {
    // noop
  }

  override def run(): PartialFunction[Int, SuperStepFunction[Long, ValuePOC2, Unit]] = {
    case 0 =>
      new PregelSuperStepFunction(this, classOf[Unit], classOf[Long]) {
        override def func: (Vertex[Long, ValuePOC2, Unit], Iterable[Unit]) => Boolean = (v, m) => {
          // create neighbours hashset
          val neighbours =  mutable.HashSet.empty[Long]
          v.edges.foreach(x => neighbours += x.dst)
          v.value = new ValuePOC2(0.0, neighbours.toSet)
          sendAll(v, v.id)
          false
        }
      }
    case 1 =>
      new QueryAnswerProcessSuperStepFunctionNoPregelMessage(this, classOf[InquiryPOC2], classOf[Int]) {
        override def query(v: Vertex[Long, ValuePOC2, Unit]): Iterable[(Long, InquiryPOC2)] = {
          if (LazySize.sizeGreaterThan(v.edges, 1)) {
            neighbours.clear()
            v.edges.foreach(x => neighbours += x.dst)

            inquiry.src = v.id
            inquiry.edges = neighbours.toArray

            v.value.lcc = neighbours.size
            new Iterable[(Long, InquiryPOC2)] {
              override def iterator: Iterator[(Long, InquiryPOC2)] = new Iterator[(Long, InquiryPOC2)] {
                val nIt =  neighbours.iterator

                override def hasNext: Boolean = nIt.hasNext

                override def next(): (Long, InquiryPOC2) = (nIt.next(), inquiry)
              }
            }

          } else {
            emptyIterable
          }
        }

        override def answer(v : Vertex[Long, ValuePOC2, Unit], query: InquiryPOC2): Int = {
          if (v.edges.isEmpty) {
            0
          } else {
            val matches = query.edges.count(v.value.neighboursSet.contains)
            matches
          }
        }

        override def process(v: Vertex[Long, ValuePOC2, Unit], data: Iterable[Int]): Boolean = {
          if (data.isEmpty) {
            v.value.lcc = 0.0
          } else {
            data.sum match {
              case 0 =>
                v.value.lcc = 0
              case matches =>
                val numNeighbours : Double = v.value.lcc
                val lcc = matches / numNeighbours / (numNeighbours - 1)
                v.value.lcc = lcc
            }
          }
          true
        }

      }
  }
}

class ValuePOC2 extends KryoSerializable {
  var lcc : Double = _
  var neighboursSet : Set[Long] = _
  def this(lcc : Double, neighboursSet : Set[Long]) = {
    this()
    this.lcc = lcc
    this.neighboursSet = neighboursSet
  }

  override def toString: String = lcc.toString

  override def read(kryo: Kryo, input: Input): Unit = {
    lcc = input.readDouble()
    neighboursSet = Set.empty[Long]
    val numNeighbours = input.readInt()
    var i = 0
    while (i < numNeighbours) {
      neighboursSet += input.readLong()
      i += 1
    }
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeDouble(lcc)
    output.writeInt(neighboursSet.size)
    neighboursSet.foreach(output.writeLong)
  }
}

class InquiryPOC2 extends KryoSerializable {
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
    val edgesArr = new Array[Long](input.readInt())
    var i = 0
    while (i < edgesArr.length) {
      edgesArr(i) = input.readLong()
      i += 1
    }
    edges = edgesArr
  }

  override def toString: String = {
    val sb = new StringBuilder
    edges.foreach(v => sb.append(v + " "))
    sb.toString()
  }
}