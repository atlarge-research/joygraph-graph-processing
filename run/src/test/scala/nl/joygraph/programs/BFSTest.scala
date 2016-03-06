package nl.joygraph.programs

import nl.joygraph.JoyGraphTestBuilder
import nl.joygraph.core.actor.{Master, Worker}
import nl.joygraph.core.partitioning.impl.VertexHashPartitioner
import nl.joygraph.core.program.NullClass
import nl.joygraph.impl.hadoop.actor.HadoopMaster
import org.scalatest.FunSuite

class BFSTest extends FunSuite {
  test("wut wut") {
    val instance = JoyGraphTestBuilder(classOf[BFS])
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, parser, programClass, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, parser, programClass, partitioner))
        .vertexPartitioner(new VertexHashPartitioner())
        .dataPath("/home/sietse/amazon0302.e")
        .parser((l : String) => {
          val s = l.split("\\s")
          (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
        })
        .programParameters(("source_id", "99843"))
        .numWorkers(1)
      .build()
    instance.run()
  }
}
