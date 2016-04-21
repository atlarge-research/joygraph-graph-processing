package io.joygraph.programs

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.typesafe.config.Config
import io.joygraph.core.program.{Vertex, _}

import scala.collection.mutable

object CDLP {
  val MAX_INTERATIONS_CONF_KEY = "maxIterations"
}

class UCDLP extends NewVertexProgram[Long, Long, Unit] {

  private[this] var maxIterations : Int = _

  override def load(conf: Config): Unit = {
    maxIterations = conf.getInt(CDLP.MAX_INTERATIONS_CONF_KEY)
  }

  private[this] val labelOccurences = new mutable.OpenHashMap[Long, Long]().withDefaultValue(0L)

  override def run(): PartialFunction[Int, SuperStepFunction[Long, Long, Unit]] = {
    case 0 => new PregelSuperStepFunction(this, classOf[Long], classOf[Long]) {
      override def func: (Vertex[Long, Long, Unit], Iterable[Long]) => Boolean = (v, m) => {
        v.value = v.id
        propagateLabel(this, v)
        false
      }
    }
    case superStep : Int => new PregelSuperStepFunction(this, classOf[Long], classOf[Long]) {
      override def func: (Vertex[Long, Long, Unit], Iterable[Long]) => Boolean = (v, m) => {
        superStep match {
          case _ =>
            determineLabel(v, m)
            if (superStep >= (maxIterations + initialisationSteps) - 1) {
              true
            } else {
              propagateLabel(this, v)
              false
            }

        }
      }

      private[this] val initialisationSteps = 1

      private[this] def determineLabel(v : Vertex[Long, Long, Unit], incomingLabels : Iterable[Long]) = {
        if (incomingLabels.nonEmpty) {
          labelOccurences.clear()
          // Compute for each incoming label the aggregate and maximum scores
          incomingLabels.foreach{ message =>
            labelOccurences += message -> (labelOccurences(message) + 1)
          }

          // Find the label with the highest frequency score (primary key) and lowest id (secondary key)
          var bestLabel = 0L
          var highestFrequency = 0L
          labelOccurences.foreach {
            case (label, frequency)
              if frequency > highestFrequency ||
                (frequency == highestFrequency && label < bestLabel) =>
              bestLabel = label
              highestFrequency = frequency
            case _ =>
            // noop
          }

          // Update the label of this vertex
          v.value = bestLabel
        }

      }
    }
  }

  private[this] def propagateLabel(f : PregelSuperStepFunction[Long, Long, Unit, _ <: Any, Long], v : Vertex[Long, Long, Unit]) = {
    f.sendAll(v, v.value)
  }
}

object DCDLP {
  val UNIDIRECTIONAL = false
  val BIDIRECTIONAL = true
}

class DCDLP extends NewVertexProgram[Long, Long, Boolean] {

  private[this] var maxIterations : Int = _

  override def load(conf: Config): Unit = {
    maxIterations = conf.getInt(CDLP.MAX_INTERATIONS_CONF_KEY)
  }

  private[this] val labelMessage = new LabelMessage
  private[this] val messageSet = new mutable.HashSet[Long]()
  private[this] val labelOccurences = new mutable.OpenHashMap[Long, Long]().withDefaultValue(0L)

  override def run(): PartialFunction[Int, SuperStepFunction[Long, Long, Boolean]] = {
    case 0 => new PregelSuperStepFunction(this, classOf[Long], classOf[Long]) {
      override def func: (Vertex[Long, Long, Boolean], Iterable[Long]) => Boolean = (v, m) => {
        sendAll(v, v.id)
        false
      }
    }
    case 1 => new PregelSuperStepFunction(this, classOf[Long], classOf[LabelMessage]) {
      override def func: (Vertex[Long, Long, Boolean], Iterable[Long]) => Boolean = (v, m) => {
        messageSet.clear()
        messageSet ++= m
        v.mutableEdges.foreach {
          case edge @ Edge(dst, _) if messageSet.contains(dst) =>
            messageSet.remove(dst)
            edge.e = DCDLP.BIDIRECTIONAL
          case _ =>
          // noop
        }

        messageSet.foreach(v.addEdge(_, DCDLP.UNIDIRECTIONAL))

        v.value = v.id
        // pass mutable edges as the readOnly v.edges does not contain the updated view!
        propagateLabel(this, v, v.mutableEdges)
        false
      }
    }
    case superStep : Int => new PregelSuperStepFunction(this, classOf[LabelMessage], classOf[LabelMessage]) {
      override def func: (Vertex[Long, Long, Boolean], Iterable[LabelMessage]) => Boolean = (v, m) => {
        superStep match {
          case _ =>
            determineLabel(v, m)
            if (superStep >= (maxIterations + initialisationSteps) - 1) {
              true
            } else {
              propagateLabel(this, v, v.edges)
              false
            }

        }
      }

      private[this] val initialisationSteps = 2

      private[this] def determineLabel(v : Vertex[Long, Long, Boolean], incomingLabels : Iterable[LabelMessage]) = {
        if (incomingLabels.nonEmpty) {
          labelOccurences.clear()
          // Compute for each incoming label the aggregate and maximum scores
          incomingLabels.foreach{ message =>
            labelOccurences += message.label -> (labelOccurences(message.label) + message.count)
          }

          // Find the label with the highest frequency score (primary key) and lowest id (secondary key)
          var bestLabel = 0L
          var highestFrequency = 0L
          labelOccurences.foreach {
            case (label, frequency)
              if frequency > highestFrequency ||
                (frequency == highestFrequency && label < bestLabel) =>
              bestLabel = label
              highestFrequency = frequency
            case _ =>
            // noop
          }

          // Update the label of this vertex
          v.value = bestLabel
        }

      }
    }
  }

  private[this] def propagateLabel(f : PregelSuperStepFunction[Long, Long, Boolean, _ <: Any, LabelMessage], v : Vertex[Long, Long, Boolean], edges : Iterable[Edge[Long, Boolean]]) = {
    val m = v.value
    edges.foreach{
      case Edge(dst, value) =>
        labelMessage.label = m
        if (value == DCDLP.UNIDIRECTIONAL) {
          labelMessage.count = 1
        }
        if (value == DCDLP.BIDIRECTIONAL) {
          labelMessage.count = 2
        }
        f.send(labelMessage, dst)
    }
  }
}

class LabelMessage extends KryoSerializable {
  var label : Long = _
  var count : Int = _
  def this(label : Long, count : Int) = {
    this()
    this.label = label
    this.count = count
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeLong(label)
    output.writeInt(count)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    label = input.readLong()
    count = input.readInt()
  }

  override def toString: String = "label: " + label + " " + "count: " + count
}
