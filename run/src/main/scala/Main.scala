import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import nl.joygraph.core.AppFactoryProvider
import nl.joygraph.core.actor.{BaseActor, Master}
import nl.joygraph.programs.BFS

object Main extends App{
  val cfg = (port : Int, seedPort : Int) => s"""
  akka {
      actor {
        provider = "akka.cluster.ClusterActorRefProvider"
      }
      remote {
        netty.tcp {
            hostname = "127.0.0.1"
            maximum-frame-size = 2M
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

  val jobCfg =
    """
      job {
        workers.initial = 3
        data.path = "file:///home/sietse/cit-Patents-edge.txt"
      }
      input.format.class = "EdgeListFormatLong"
      fs.defaultFS = "file:///"
      vertex.program.class = "BFS"
      worker {
        suffix = "worker"
        input.lineProviderClass = "nl.joygraph.impl.hadoop.reader.HadoopLineProvider"
      }
      master.suffix = "master"
    """

  val jobConfig = ConfigFactory.parseString(jobCfg)

//  val appBuilder = AppFactoryProvider.create(classOf[BFS])
//  appBuilder.initializeTypeTags()

  val masterFactory = (cluster : Cluster) => {
    Master.create(classOf[nl.joygraph.impl.hadoop.actor.Master], jobConfig, cluster)
  }

  val workerFactory = AppFactoryProvider.workerFactory(
    jobConfig,
    classOf[BFS]
//    appBuilder.clazzI,
//    appBuilder.clazzV,
//    appBuilder.clazzE,
//    appBuilder.clazzM
  )

  val config = ConfigFactory.parseString(cfg(2552, 2552))
  val system = ActorSystem("clustertest", config)
  system.actorOf(Props(classOf[BaseActor], jobConfig, masterFactory, workerFactory))

  val config2 = ConfigFactory.parseString(cfg(2553, 2552))
  val system2 = ActorSystem("clustertest", config2)
  system2.actorOf(Props(classOf[BaseActor], jobConfig, masterFactory, workerFactory))

  val config3 = ConfigFactory.parseString(cfg(2554, 2552))
  val system3 = ActorSystem("clustertest", config3)
  system3.actorOf(Props(classOf[BaseActor],  jobConfig, masterFactory, workerFactory))
}
