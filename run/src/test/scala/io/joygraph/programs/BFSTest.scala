package io.joygraph.programs

import java.nio.charset.StandardCharsets

import io.joygraph.core.actor.{Master, Worker}
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import io.joygraph.core.program.NullClass
import io.joygraph.core.runner.JoyGraphLocalInstanceBuilder
import io.joygraph.impl.hadoop.actor.HadoopMaster
import org.scalatest.FunSuite

class BFSTest extends FunSuite {
  //  val file = "/home/sietse/amazon0302.e"
  //  val source_id = "99843"
  val file = "/home/sietse/cit-Patents-edge.txt"
  val source_id = "4949326"

  test("BreadthFirstSearch test") {
    val instance = JoyGraphLocalInstanceBuilder(classOf[BFS])
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, parser, outputWriter, programClass, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, parser, outputWriter, programClass, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .parser((l) => {
        val s = l.split("\\s")
        (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
      })
      .writer((v, outputStream) => {
        outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8))
      })
      .programParameters(("source_id", source_id))
      .numWorkers(8)
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch combinable test") {
    val instance = JoyGraphLocalInstanceBuilder(classOf[BFSCombinable])
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, parser, outputWriter, programClass, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, parser, outputWriter, programClass, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .parser((l) => {
        val s = l.split("\\s")
        (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
      })
      .writer((v, outputStream) => {
        outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8))
      })
      .programParameters(("source_id", source_id))
      .numWorkers(2)
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch aggregatable test") {
    val instance = JoyGraphLocalInstanceBuilder(classOf[BFSAggregatable])
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, parser, outputWriter, programClass, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, parser, outputWriter, programClass, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .parser((l) => {
        val s = l.split("\\s")
        (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
      })
      .writer((v, outputStream) => {
        outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8))
      })
      .programParameters(("source_id", source_id))
      .numWorkers(2)
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }
}
