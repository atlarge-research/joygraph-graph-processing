package io.joygraph.core.runner

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import io.joygraph.core.actor.{BaseActor, Master, Worker}
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program.ProgramDefinition
import io.joygraph.core.util.net.PortFinder

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object JoyGraphLocalInstanceBuilder {
  def apply[I,V,E](programDefinition: ProgramDefinition[String, I,V,E]) : JoyGraphLocalInstanceBuilder[I,V,E] = {
    new JoyGraphLocalInstanceBuilder[I,V,E](programDefinition)
  }
}

class JoyGraphLocalInstanceBuilder[I,V,E](programDefinition: ProgramDefinition[String, I,V,E]) {

  protected[this] type BuilderType = JoyGraphLocalInstanceBuilder[I,V,E]
  protected[this] var _workers : Option[Int] = None
  protected[this] var _dataPath : Option[String] = None
  protected[this] var _workerFactory : Option[(Config, ProgramDefinition[String, _,_,_], VertexPartitioner) => Worker[_,_,_]] = None
  protected[this] var _masterFactory : Option[(Config, Cluster) => Master] = None
  protected[this] var _partitioner : Option[VertexPartitioner] = None
  protected[this] var _programParameters : ArrayBuffer[(String,String)] = ArrayBuffer.empty
  protected[this] var _outputPath : Option[String] = None

  def programParameters(keyValue: (String, String)) : BuilderType = {
    _programParameters += keyValue
    this
  }

  def outputPath(path: String) : BuilderType = {
    _outputPath = Option(path)
    this
  }

  def numWorkers(workers : Int): BuilderType = {
    _workers = Some(workers)
    this
  }

  def dataPath(path : String) : BuilderType = {
    _dataPath = Option(path)
    this
  }

  def masterFactory(masterFactory : (Config, Cluster) => Master)  : BuilderType = {
    _masterFactory = Option(masterFactory)
    this
  }

  def workerFactory(workerFactory :
                    (Config,
                      ProgramDefinition[String, _,_,_],
                      VertexPartitioner) => Worker[_,_,_]) : BuilderType = {
    _workerFactory = Option(workerFactory)
    this
  }

  def vertexPartitioner(partitioner : VertexPartitioner) : BuilderType = {
    _partitioner = Option(partitioner)
    this
  }

  def build() : JoyGraphLocalInstance = {
    val graphTestInstance = new JoyGraphLocalInstance(programDefinition)

    _dataPath match {
      case Some(dataPath) => graphTestInstance.dataPath(dataPath)
      case None => throw new IllegalArgumentException("Missing path")
    }

    _workers match {
      case Some(workers) => graphTestInstance.numWorkers(workers)
      case None => throw new IllegalArgumentException("Missing number of workers")
    }

    _masterFactory match {
      case Some(masterFactory) => graphTestInstance.masterFactory(masterFactory)
      case None => throw new IllegalArgumentException("Missing master factory")
    }

    _workerFactory match {
      case Some(workerFactory) => graphTestInstance.workerFactory(workerFactory)
      case None => throw new IllegalArgumentException("Missing worker factory")
    }

    _partitioner match {
      case Some(partitioner) => graphTestInstance.vertexPartitioner(partitioner)
      case None => throw new IllegalArgumentException("Missing vertex partitioner")
    }

    graphTestInstance.programParameters(_programParameters)

    _outputPath match {
      case Some(outputPath) => graphTestInstance.outputPath(outputPath)
      case None => throw new IllegalArgumentException("Missing output path")
    }

    graphTestInstance
  }
}

protected[this] class JoyGraphLocalInstance(programDefinition : ProgramDefinition[String, _,_,_]) {
  protected[this] type Type = JoyGraphLocalInstance
  protected[this] var _workers : Int = _
  protected[this] var _dataPath : String = _
  protected[this] var _workerFactory : (Config, ProgramDefinition[String, _,_,_], VertexPartitioner) => Worker[_,_,_] = _
  protected[this] var _masterFactory : (Config, Cluster) => Master = _
  protected[this] var _partitioner : VertexPartitioner = _
  protected[this] var _programParameters : ArrayBuffer[(String, String)] = _
  protected[this] var _outputPath : String = _

  def programParameters(programParameters : ArrayBuffer[(String, String)]) : Type = {
    _programParameters = programParameters
    this
  }

  def numWorkers(workers : Int): Type = {
    _workers = workers
    this
  }

  def dataPath(path : String) : Type = {
    _dataPath = path
    this
  }

  def outputPath(path: String) : Type = {
    _outputPath = path
    this
  }

  def masterFactory(masterFactory : (Config, Cluster) => Master)  : Type = {
    _masterFactory = masterFactory
    this
  }

  def workerFactory(workerFactory :
                    (Config,
                      ProgramDefinition[String, _,_,_],
                      VertexPartitioner) => Worker[_,_,_]) : Type = {
    _workerFactory = workerFactory
    this
  }

  def vertexPartitioner(partitioner : VertexPartitioner) : Type = {
    _partitioner = partitioner
    this
  }

  def run(): Unit = {
    val actorSystemName = "JoyGraphTest"
    val cfg = (port: Int, seedPort: Int) =>
      s"""
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
            "akka.tcp://$actorSystemName@127.0.0.1:$seedPort"
          ]
        auto-down = on
      }
  }"""

    val jobCfg =
      s"""
      job {
        program.definition.class = ${programDefinition.getClass.getName}
        master.memory = 1000
        master.cores = 1
        worker.memory = 2000
        worker.cores = 1
        workers.initial = ${_workers}
        data.path = "file://${_dataPath}"
        output.path = "file://${_outputPath}"
      }
      worker {
        suffix = "worker"
        output.lineWriterClass = "io.joygraph.impl.hadoop.writer.HadoopLineWriter"
        input.lineProviderClass = "io.joygraph.impl.hadoop.reader.HadoopLineProvider"
      }
      master.suffix = "master"
    """ + {
        if (_programParameters.isEmpty)
          "\n"
        else
          _programParameters.map(kv => s"""${kv._1} = ${kv._2}\n""").reduce(_ + "" + _)
      }
    val jobConfig = ConfigFactory.parseString(jobCfg)

    val seedPort = PortFinder.findFreePort(2552)
    var port = seedPort
    (0 until (_workers + 1)).map { _ =>
      val config = ConfigFactory.parseString(cfg(port, seedPort))
      val system = ActorSystem(actorSystemName, config)
      system.actorOf(Props(classOf[BaseActor], jobConfig, _masterFactory, () => _workerFactory(jobConfig, programDefinition, _partitioner)))
      port = PortFinder.findFreePort(port + 1)
      system.whenTerminated
    }.foreach(Await.ready(_, Duration(Int.MaxValue, TimeUnit.MILLISECONDS)))
  }
}
