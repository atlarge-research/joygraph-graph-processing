import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import nl.joygraph.core.actor.{BaseActor, Master, Worker}
import nl.joygraph.core.partitioning.impl.VertexHashPartitioner
import nl.joygraph.core.program.{NullClass, Vertex}
import nl.joygraph.impl.hadoop.actor.HadoopMaster
import nl.joygraph.programs.BFS

object Main extends App{
  val cfg = (port : Int, seedPort : Int) => s"""
  akka {
      actor {
        provider = "akka.cluster.ClusterActorRefProvider"
      }
      remote {
        watch-failure-detector.acceptable-heartbeat-pause = 10
        netty.tcp {
            hostname = "127.0.0.1"
            maximum-frame-size = 10M
            port = $port
        }
      }
      cluster {
        seed-nodes = [
            "akka.tcp://clustertest@127.0.0.1:$seedPort"
          ]
        auto-down = on
      }
  }"""

  val numWorkers = 2

  val sourceId = 99843
  val dataset = "/home/sietse/amazon0302.e"
//  val dataset = "/home/sietse/cit-Patents-edge.txt"
//  val sourceId = 4949326
  val jobCfg =
    s"""
      job {
        workers.initial = $numWorkers
        data.path = "file://$dataset"
      }
      fs.defaultFS = "file:///"
      vertex.program.class = "BFS"
      worker {
        suffix = "worker"
        input.lineProviderClass = "nl.joygraph.impl.hadoop.reader.HadoopLineProvider"
      }
      master.suffix = "master"
      source_id = $sourceId
    """

  val jobConfig = ConfigFactory.parseString(jobCfg)
  val programClass = classOf[BFS]

  val parser : (String) => (Long, Long, NullClass) = (l : String) => {
//    val Array(a,b) = l.split("\\s")
//    (a.toLong, b.toLong, NullClass.SINGLETON)
    val s = l.split("\\s")
    (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
  }

  val output = (v : Vertex[Long, Int, NullClass, Long]) => {
    v.edges
  }

  val masterFactory = (cluster : Cluster) => {
    val master = new Master(jobConfig, cluster) with HadoopMaster
    Master.initialize(master)
  }

  val workerFactory = Worker.workerWithTrieMapMessageStore(
    jobConfig,
    parser,
    programClass,
    new VertexHashPartitioner(numWorkers)
  )

  val seedPort = 2552
  var port = 2552
  for (i <- 0 until numWorkers) {
    val config = ConfigFactory.parseString(cfg(port, seedPort))
    val system = ActorSystem("clustertest", config)
    system.actorOf(Props(classOf[BaseActor], jobConfig, masterFactory, workerFactory))
    port += 1
  }


}
