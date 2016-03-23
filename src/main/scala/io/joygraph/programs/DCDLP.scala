package io.joygraph.programs

import com.typesafe.config.Config
import io.joygraph.core.program.{Vertex, _}

import scala.collection.mutable

object DCDLP {
  val UNIDIRECTIONAL = false
  val BIDIRECTIONAL = true
  val MAX_INTERATIONS_CONF_KEY = "maxIterations"
}

class DCDLP extends NewVertexProgram[Long, Long, Boolean] {

  private[this] var maxIterations : Int = _

  override def load(conf: Config): Unit = {
    maxIterations = conf.getInt(DCDLP.MAX_INTERATIONS_CONF_KEY)
  }

  private[this] val messageSet = new mutable.HashSet[Long]()
  private[this] val labelOccurences = new mutable.OpenHashMap[Long, Long]().withDefaultValue(0L)

  override def run(): PartialFunction[Int, SuperStepFunction[Long, Long, Boolean, _, _]] = {
    case superStep : Int => new SuperStepFunction(this, classOf[Long], classOf[Long]) {
      override def func: (Vertex[Long, Long, Boolean], Iterable[Long]) => Boolean = (v, m) => {
        superStep match {
          case 0 | 1 =>
            initialisationStep(v, m, superStep)
          case _ =>
            determineLabel(v, m)
            if (superStep >= (maxIterations + initialisationSteps) - 1) {
              true
            } else {
              propagateLabel(v, v.edges)
              false
            }

        }
      }

      private[this] val initialisationSteps = 2

      private[this] def initialisationStep(v : Vertex[Long, Long, Boolean], messages : Iterable[Long], superStep : Int): Boolean = {
        superStep match {
          case 0 =>
            sendAll(v, v.id)
            false
          case 1 =>
            messageSet.clear()
            messageSet ++= messages
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
            propagateLabel(v, v.mutableEdges)
            false
        }
      }

      private[this] def determineLabel(v : Vertex[Long, Long, Boolean], incomingLabels : Iterable[Long]) = {
        if (incomingLabels.nonEmpty) {
          labelOccurences.clear()
          // Compute for each incoming label the aggregate and maximum scores
          incomingLabels.foreach(label => labelOccurences += label -> (labelOccurences(label) + 1))

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

      private[this] def propagateLabel(v : Vertex[Long, Long, Boolean], edges : Iterable[Edge[Long, Boolean]]) = {
        val m = v.value
        edges.foreach{
          case Edge(dst, value) =>
            send(m, dst)
            if (value == DCDLP.BIDIRECTIONAL) {
              send(m, dst)
            }
        }
      }
    }
  }
}
