package io.joygraph.programs

import java.nio.charset.StandardCharsets

import io.joygraph.core.actor.{Master, Worker}
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import io.joygraph.core.program.{NullClass, ProgramDefinition, Vertex}
import io.joygraph.core.runner.JoyGraphLocalInstanceBuilder
import io.joygraph.definitions._
import io.joygraph.impl.hadoop.actor.HadoopMaster
import org.scalatest.FunSuite

class BFSTest extends FunSuite {
//    val file = "/home/sietse/amazon0302.e"
//    val vfile = "/home/sietse/amazon0302.v"
//    val source_id = "99843"
//  val file = "/home/sietse/cit-Patents-edge.txt"
//  val source_id = "4949326"
//  val file = "/home/sietse/dota-league.e"
//  val source_id = "287770"
  val file ="/home/sietse/wiki-Talk.e"
  val vfile ="/home/sietse/wiki-Talk.v"
  val source_id = "2"
//  val file = "/home/sietse/datagen-100-fb-cc0_05.e"
//val source_id = "594259"

  test("DLCC elastic test") {
    val programDefinition = new DLCCEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStore(config, programDefinition, partitioner))
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStoreWithVerticesStore(config, programDefinition, partitioner))
      //            .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStoreWithSerializedVerticesStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(1)
      .outputPath("/home/sietse/outputPathELASTICLCC")
      .elastic()
      .directed()
      .build()
    instance.run()
  }

  test("DCDLP test") {
    val programDefinition = new DCDLPEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
//            .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStore(config, programDefinition, partitioner))
      .programParameters(("maxIterations", "2"))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(3)
      .outputPath("/home/sietse/outputPathDCDLP2")
      .directed()
      .build()
    instance.run()
  }

  test("DLCC test") {
    val programDefinition = new DLCCEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
//      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStore(config, programDefinition, partitioner))
//      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStoreWithVerticesStore(config, programDefinition, partitioner))
//            .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStoreWithSerializedVerticesStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(4)
      .directed()
      .outputPath("/home/sietse/outputPathLCC")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch test programDef nonserialized") {
    val programDefinition = new BFSEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .programParameters(("source_id", source_id))
      .numWorkers(4)
      .directed()
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch test programDef") {
    val programDefinition : ProgramDefinition[String,_, _,_] = Class.forName(classOf[BFSEdgeListDefinition].getName).newInstance().asInstanceOf[ProgramDefinition[String,_,_,_]]
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .programParameters(("source_id", source_id))
      .numWorkers(4)
      .directed()
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }

  test("DWCC") {
    val programDefinition = new DWCCEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .programParameters(("none", "none"))
      .numWorkers(4)
      .directed()
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }

  test("Pagerank test") {
    val programDefinition = new PREdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .programParameters(("dampingFactor", "0.9"))
      .programParameters(("numIterations", "2"))
      .numWorkers(4)
      .directed()
      .outputPath("/home/sietse/outputPath5")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch test") {
    val instance = JoyGraphLocalInstanceBuilder(
      ProgramDefinition(
        (l) => {
          val s = l.split("\\s")
          (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
        },
        (l) => l.toLong,
        (v : Vertex[_,_,_], outputStream) => {
          outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8))
        },
        classOf[BFS]
      ))
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .vertexPath(vfile)
      .programParameters(("source_id", source_id))
      .numWorkers(8)
      .directed()
      .outputPath("/home/sietse/outputPathWikitalk8")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch combinable test") {
    val instance = JoyGraphLocalInstanceBuilder(
      ProgramDefinition(
        (l) => {
          val s = l.split("\\s")
          (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
        },
        (l) => l.toLong,
        (v : Vertex[_,_,_], outputStream) => {
          outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8))
        },
        classOf[BFSCombinable]
      ))
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .programParameters(("source_id", source_id))
      .numWorkers(2)
      .directed()
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch aggregatable test") {
    val instance = JoyGraphLocalInstanceBuilder(
      ProgramDefinition(
        (l) => {
          val s = l.split("\\s")
          (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
        },
        (l) => l.toLong,
        (v : Vertex[_,_,_], outputStream) => {
          outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8))
        },
        classOf[BFSCombinable]
      ))
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .programParameters(("source_id", source_id))
      .numWorkers(2)
      .directed()
      .outputPath("/home/sietse/outputPath")
      .build()
    instance.run()
  }
}
