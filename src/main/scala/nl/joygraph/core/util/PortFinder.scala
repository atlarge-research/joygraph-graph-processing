package nl.joygraph.core.util

import java.net.ServerSocket

import scala.util.{Failure, Success, Try}

/**
  * Finds a port between MIN_PORT and MAX_PORT
  */
object PortFinder {
  // TODO range should be adjustable through configuration
  val MIN_PORT = 49152
  val MAX_PORT = 65535

  def findFreePort(targetPort : Int = MIN_PORT): Int = {
    var currentPort = targetPort
    while (checkPort(currentPort) == -1) {
      currentPort += 1
      currentPort %= MAX_PORT
      currentPort = Math.max(currentPort, MIN_PORT)
    }
    currentPort
  }

  private def checkPort(targetPort : Int) : Int =  synchronized {
    Try {
      new ServerSocket(targetPort)
    } match {
      case Failure(exception) =>
        -1
      case Success(socket) =>
        Try {
          socket.setReuseAddress(true)
        } match {
          case Failure(exception) =>
            Try {
              socket.close()
            } match {
              case _ => -1
            }
          case Success(_) =>
            // woop it's free! close the damn socket
            Try {
              socket.close()
            } match {
              case Failure(exception) =>
                -1
              case Success(value) =>
                targetPort
            }
        }
    }

  }
}
