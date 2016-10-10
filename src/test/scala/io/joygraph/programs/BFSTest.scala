package io.joygraph.programs

import java.nio.charset.StandardCharsets

import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Result}
import io.joygraph.core.actor.metrics.WorkerOperation
import io.joygraph.core.actor.{Master, Worker}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import io.joygraph.core.program.{ProgramDefinition, Vertex}
import io.joygraph.core.runner.JoyGraphLocalInstanceBuilder
import io.joygraph.core.util.ParseUtil
import io.joygraph.definitions._
import io.joygraph.impl.hadoop.actor.HadoopMaster
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.io.Source
import scala.reflect.io.{Directory, File}

class TestPolicy extends ElasticPolicy {
  override def init(policyParams: Config): Unit = {}

  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
    Some(Grow(Iterable(currentWorkers.size), new VertexHashPartitioner(currentWorkers.size + 1)))
  }
}

object BFSTest {
  def validatOutput(valid : String, toBeTested : String) = {
    val validRes = mutable.OpenHashMap.empty[Long, Double]
    val toBeTestedRes = mutable.OpenHashMap.empty[Long, Double]

    Source.fromFile(valid).getLines().foreach{l =>
      val s = l.split(" ")
      validRes(s(0).toLong) = s(1).toDouble
    }

    Directory(toBeTested).files.filterNot(_.name.endsWith(".crc"))foreach { file =>
      Source.fromFile(file.jfile).getLines().foreach{l =>
        val s = l.split(" ")
        val key = s(0).toLong
        val expected = validRes(key)
        val actual = s(1).toDouble
        toBeTestedRes(key) = actual
        //      println(s"${s(0)} is ${s(1)}, expected $expected")
        if(math.abs(expected - actual) > 1E-4D) {
          println(s"${s(0)} is ${s(1)}, expected $expected")
        }
      }
    }

    validRes.keys.foreach{
      key =>
        if (!toBeTestedRes.contains(key)) {
          println(key)
        }
    }
  }
}

class BFSTest extends FunSuite {
    val file = "/home/sietse/amazon0302.e"
    val vfile = "/home/sietse/amazon0302.v"
    val source_id = "99843"
//  val file = "/home/sietse/cit-Patents-edge.txt"
//  val source_id = "4949326"
//  val file = "/home/sietse/dota-league.e"
//  val source_id = "287770"
//  val file ="/home/sietse/wiki-Talk.e"
//  val vfile ="/home/sietse/wiki-Talk.v"
//  val source_id = "2"
//  val file = "/home/sietse/datagen-100.e"
//  val vfile = "/home/sietse/datagen-100.v"
//val source_id = "594259"

  test("Metrics serialization deserialization") {
    val programDefinition = new DWCCEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializeOpenHashMapStore(config, programDefinition, partitioner))
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStore(config, programDefinition, partitioner))
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStoreWithVerticesStore(config, programDefinition, partitioner))
      //            .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStoreWithSerializedVerticesStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(2)
      .workerCores(1)
      .outputPath("/home/sietse/outputPathAmazon")
      .programParameters(("source_id", source_id))
      .programParameters(("job.workers.max", 10.toString))
      .programParameters(("job.metrics.persistence.file.path", "/tmp/metrics.bin"))
      .directed()
      .build()
    instance.run()

    val policy = ElasticPolicy.default()
    policy.importMetrics("/tmp/metrics.bin")
    policy.metricsOf(4, 0, WorkerOperation.RUN_SUPERSTEP).foreach{
      x => x.foreach(println)
    }
  }

  test("PR elastic test") {
//    val file = "/home/sietse/dota-league.e"
    val file = "/home/sietse/example-undirected.e"
    val validOutput = "/home/sietse/example-undirected-PR"
//    val validOutput = "/home/sietse/dota-league-PR"

    // clear output
    val outputPath = "/home/sietse/PRtestOutput"
    Directory(outputPath).deleteRecursively()

    val programDefinition = new PREdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
//      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializeOpenHashMapStore(config, programDefinition, partitioner))
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStore(config, programDefinition, partitioner))
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStoreWithVerticesStore(config, programDefinition, partitioner))
      //            .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStoreWithSerializedVerticesStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(2)
      .workerCores(8)
      .outputPath(outputPath)
      .elastic()
      .programParameters(("source_id", source_id))
      .programParameters(("dampingFactor", 0.85.toString))
      .programParameters(("numIterations", 2.toString))
      .programParameters(("job.workers.max", 4.toString))
      .policy(new TestPolicy)
      .undirected()
      .build()
    instance.run()

    BFSTest.validatOutput(validOutput, outputPath)
  }

  test("DCDLP test") {
    val programDefinition = new DCDLPEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
//      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
            .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializeOpenHashMapStore(config, programDefinition, partitioner))
      .programParameters(("maxIterations", "3"))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(1)
//      .workerCores(2)
      .outputPath("/home/sietse/outputPathDCDLP2")
      .directed()
      .build()
    instance.run()
  }

  test("DLCC test") {
    val programDefinition = new DLCCPOC2EdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializeOpenHashMapStore(config, programDefinition, partitioner))
//      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStore(config, programDefinition, partitioner))
//      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStoreWithVerticesStore(config, programDefinition, partitioner))
//            .workerFactory((config, programDefinition, partitioner) => Worker.workerWithTrieMapMessageStoreWithSerializedVerticesStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(2)
//      .workerCores(2)
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
//      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializeOpenHashMapStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .programParameters(("dampingFactor", "0.85"))
      .programParameters(("numIterations", "10"))
      .numWorkers(1)
      .undirected()
      .outputPath("/home/sietse/outputPath5")
      .build()
    instance.run()
  }

  test("DLCC POC test") {
    val programDefinition = new DLCCPOCEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializeOpenHashMapStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(2)
      .undirected()
      .outputPath("/home/sietse/DLCCPOC")
      .build()
    instance.run()
  }

  test("ULCC POC test") {
    val programDefinition = new ULCCPOCEdgeListDefinition
    val instance = JoyGraphLocalInstanceBuilder(programDefinition)
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
      //      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializeOpenHashMapStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .numWorkers(2)
      .undirected()
      .outputPath("/home/sietse/ULCCPOC")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch test") {
    val instance = JoyGraphLocalInstanceBuilder(
      ProgramDefinition(
        ParseUtil.edgeListLineLongLong,
        (l) => l.toLong,
        (v : Vertex[_,_,_], outputStream) => {
          outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8))
        },
        classOf[BFS]
      ))
      .masterFactory((jobConfig, cluster) => {
        val master = new Master(jobConfig, cluster) with HadoopMaster
        Master.initialize(master)})
//      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializedTrieMapMessageStore(config, programDefinition, partitioner))
      .workerFactory((config, programDefinition, partitioner) => Worker.workerWithSerializeOpenHashMapStore(config, programDefinition, partitioner))
      .vertexPartitioner(new VertexHashPartitioner())
      .dataPath(file)
      .vertexPath(vfile)
      .programParameters(("source_id", source_id))
      .numWorkers(1)
      .directed()
      .outputPath("/home/sietse/amazonbfs")
      .build()
    instance.run()
  }

  test("BreadthFirstSearch combinable test") {
    val instance = JoyGraphLocalInstanceBuilder(
      ProgramDefinition(
        ParseUtil.edgeListLineLongLong,
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
        ParseUtil.edgeListLineLongLong,
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
