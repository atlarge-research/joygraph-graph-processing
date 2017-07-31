package io.joygraph.analysis.external.parse

import io.joygraph.analysis.algorithm.{AlgorithmMetric, Statistics}

import scala.collection.mutable
import scala.io.Source

object GiraphMetrics {

  case class Row(workerId : Int, superStep : Int, ssTime : Long, activeVertices : Long, cpuTime : Double, memAverageStartStop : Long, wcTime : Long)

  def parse(line : String): Row = {
    val colVals = line.split(" ")
    val workerId = colVals(0).toInt
    val superStep = colVals(1).toInt
    val (ssStart, ssStop) = colVals(2).toLong -> colVals(3).toLong
    val activeVertices = colVals(4).toLong
    val (cpuStart, cpuStop) = colVals(5).toDouble -> colVals(6).toDouble
    val (memStart, memStop) = colVals(7).toLong -> colVals(8).toLong
    val (wcStart, wcEnd) = colVals(7).toLong -> colVals(8).toLong

    Row(workerId, superStep, ssStop - ssStart, activeVertices, cpuStop - cpuStart, memStop + memStart / 2, wcEnd - wcStart )
  }

  def parseFile(path : String) : AlgorithmMetric = {

    val activeVerticesPerStep = mutable.Map.empty[Int, Long]
    val wallClockPerStepPerWorker = mutable.Map.empty[Int, mutable.ArrayBuffer[(Int, Long)]]
    val activeVerticesPerStepPerWorker =  mutable.Map.empty[Int, mutable.ArrayBuffer[(Int, Long)]]
    val heapMemoryPerStepPerWorker = mutable.Map.empty[Int, mutable.ArrayBuffer[(Int, Statistics)]]
    val cpuTimePerStepPerWorker = mutable.Map.empty[Int, mutable.ArrayBuffer[(Int, Statistics)]]

    Source.fromFile(path).getLines()
      .drop(1) // skip first line
      .foreach { line =>
      val row = parse(line)
      activeVerticesPerStep.get(row.superStep) match {
        case Some(x) =>
          activeVerticesPerStep(row.superStep) = x + row.activeVertices
        case None =>
          activeVerticesPerStep(row.superStep) = row.activeVertices
      }

      wallClockPerStepPerWorker.getOrElseUpdate(row.superStep, mutable.ArrayBuffer.empty[(Int, Long)]) += row.workerId -> row.wcTime
      activeVerticesPerStepPerWorker.getOrElseUpdate(row.superStep, mutable.ArrayBuffer.empty[(Int, Long)]) += row.workerId -> row.activeVertices
      heapMemoryPerStepPerWorker.getOrElseUpdate(row.superStep, mutable.ArrayBuffer.empty[(Int, Statistics)]) += row.workerId -> Statistics(0, row.memAverageStartStop, 1)
      cpuTimePerStepPerWorker.getOrElseUpdate(row.superStep, mutable.ArrayBuffer.empty[(Int, Statistics)]) += row.workerId -> Statistics(0, row.cpuTime, 1)
    }

    AlgorithmMetric(
      activeVerticesPerStep.toIndexedSeq.sortBy(_._1).map(_._2),
      wallClockPerStepPerWorker.toIndexedSeq.sortBy(_._1).map(_._2),
      activeVerticesPerStepPerWorker.toIndexedSeq.sortBy(_._1).map(_._2),
      heapMemoryPerStepPerWorker.toIndexedSeq.sortBy(_._1).map(_._2),
      scala.collection.immutable.IndexedSeq.empty[Iterable[(Int, Statistics)]],
      scala.collection.immutable.IndexedSeq.empty[Iterable[(Int, Statistics)]],
      scala.collection.immutable.IndexedSeq.empty[Iterable[(Int, Statistics)]],
      cpuTimePerStepPerWorker.toIndexedSeq.sortBy(_._1).map(_._2)
    )
  }


}