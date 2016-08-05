package io.joygraph.impl.hadoop.actor

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import io.joygraph.core.actor.{BaseActor, Master, Worker}
import io.joygraph.core.config.JobSettings
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import io.joygraph.core.program.ProgramDefinition
import io.joygraph.core.util.net.{NetUtils, PortFinder}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object YarnBaseActor {

  def startActorSystem(jobConf : Config): Unit = {
    val actorSystemName = "actorSystemName" // TODO configurable?
    val hostName = NetUtils.getHostName
    val port = PortFinder.findFreePort()
    val remotingConf = ConfigFactory.parseString(
      s"""
         |akka {
         |  actor {
         |    provider = "akka.cluster.ClusterActorRefProvider"
         |  }
         |  remote {
         |    watch-failure-detector.acceptable-heartbeat-pause = 120 s
         |    transport-failure-detector {
         |      acceptable-heartbeat-pause = 120 s
         |    }
         |    netty.tcp {
         |      maximum-frame-size = 10M
         |      hostname = "$hostName"
         |      port = $port
         |    }
         |  }
         |}
       """.stripMargin
    )

    val workerConf = remotingConf.withFallback(jobConf)
    val definition : ProgramDefinition[String,_,_,_] = Class.forName(JobSettings(jobConf).programDefinition).newInstance().asInstanceOf[ProgramDefinition[String,_,_,_]]
    val system = ActorSystem(actorSystemName, workerConf)
    system.actorOf(Props(classOf[BaseActor], workerConf,
      (conf : Config, cluster : Cluster) => {
        val master = new Master(conf, cluster) with HadoopMaster
        Master.initialize(master)
      }, () => {
        Worker.workerWithSerializeOpenHashMapStore(workerConf, definition, new VertexHashPartitioner)
      },
      None
    ))
    // wait indefinitely until it terminates
    Await.ready(system.whenTerminated, Duration(Int.MaxValue, TimeUnit.MILLISECONDS))
    println("I have terminated")
  }

  def main(args : Array[String]): Unit = {
    val configLocation : String = args(0)

    val jobConf = ConfigFactory.parseFile(new java.io.File(configLocation))
    val maxTries = 10
    var tries = 0
    try {
      tries += 1
      startActorSystem(jobConf)
    } catch {
      case (t : Throwable) =>
        t.printStackTrace()
        if (tries < maxTries) {
          tries += 1
          startActorSystem(jobConf)
        }
    }
  }
}

class YarnBaseActor {

}
