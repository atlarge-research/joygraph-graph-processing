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
  def main(args : Array[String]): Unit = {
    val configLocation : String = args(0)

    val jobConf = ConfigFactory.parseFile(new java.io.File(configLocation))
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
         |    watch-failure-detector.acceptable-heartbeat-pause = 10
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

    val definition : ProgramDefinition[String,_, _,_,_] = Class.forName(JobSettings(jobConf).programDefinition).newInstance().asInstanceOf[ProgramDefinition[String,_,_,_,_]]
    val system = ActorSystem(actorSystemName, workerConf)
    system.actorOf(Props(classOf[BaseActor], workerConf,
      (conf : Config, cluster : Cluster) => {
        val master = new Master(conf, cluster) with HadoopMaster
        Master.initialize(master)
      }, () => {
        Worker.workerWithSerializedTrieMapMessageStore(workerConf, definition, new VertexHashPartitioner)
      }
    ))
    // wait indefinitely until it terminates
    Await.ready(system.whenTerminated, Duration(Int.MaxValue, TimeUnit.MILLISECONDS))
    println("I have terminated")
  }
}

class YarnBaseActor {

}
