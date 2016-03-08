package io.joygraph.core.message

import scala.collection.mutable.ArrayBuffer

case class WorkerMap(val workers : ArrayBuffer[AddressPair])