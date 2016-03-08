package io.joygraph.programs

import java.nio.charset.StandardCharsets

import io.joygraph.JoyGraphTestBuilder
import io.joygraph.core.actor.{Master, Worker}
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import io.joygraph.core.program.NullClass
import io.joygraph.impl.hadoop.actor.HadoopMaster
import org.scalatest.FunSuite

class BFSTest extends FunSuite {
  test("BreadthFirstSearch test") {
    val instance = JoyGraphTestBuilder(classOf[BFS])
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, parser, outputWriter, programClass, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, parser, outputWriter, programClass, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath("/home/sietse/amazon0302.e")
      .parser((l) => {
        val s = l.split("\\s")
        (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
      })
      .writer((v, outputStream) => {
        outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8))
      })
      .programParameters(("source_id", "99843"))
      .numWorkers(2)
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }
}
