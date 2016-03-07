package nl.joygraph

import java.io.OutputStream
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import nl.joygraph.core.actor.{BaseActor, Master, Worker}
import nl.joygraph.core.partitioning.VertexPartitioner
import nl.joygraph.core.partitioning.impl.VertexHashPartitioner
import nl.joygraph.core.program.{Vertex, VertexProgramLike}
import nl.joygraph.core.util.net.PortFinder

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object JoyGraphTestBuilder {
  def apply[I,V,E,M](programClazz : Class[_ <: VertexProgramLike[I,V,E,M]]) : JoyGraphTestBuilder[I,V,E,M] = {
    new JoyGraphTestBuilder[I,V,E,M](programClazz)
  }
}

class JoyGraphTestBuilder[I,V,E,M](programClazz : Class[_ <: VertexProgramLike[I,V,E,M]]) {

  private[this] type BuilderType = JoyGraphTestBuilder[I,V,E,M]
  private[this] var _workers : Option[Int] = None
  private[this] var _dataPath : Option[String] = None
  private[this] var _parser : Option[(String) => (I,I,E)] = None
  private[this] var _workerFactory : Option[(Config, (String) => (I, I, E), (Vertex[I,V,E,M], OutputStream) => Any, Class[_ <: VertexProgramLike[I,V,E,M]], VertexPartitioner) => Worker[I,V,E,M]] = None
  private[this] var _masterFactory : Option[(Config, Cluster) => Master] = None
  private[this] var _partitioner : Option[VertexPartitioner] = None
  private[this] var _programParameters : Option[(String, String)] = None
  private[this] var _outputPath : Option[String] = None
  private[this] var _writer : Option[(Vertex[I,V,E,M], OutputStream) => Any] = None

  def programParameters(keyValue: (String, String)) : BuilderType = {
    _programParameters = Option(keyValue)
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

  def writer(f : (Vertex[I,V,E,M], OutputStream) => Any) : BuilderType = {
    _writer = Option(f)
    this
  }

  def parser(f : (String) => (I, I, E)) : BuilderType = {
    _parser = Option(f)
    this
  }


  def masterFactory(masterFactory : (Config, Cluster) => Master)  : BuilderType = {
    _masterFactory = Option(masterFactory)
    this
  }

  def workerFactory(workerFactory :
                    (Config,
                      (String) => (I, I, E),
                      (Vertex[I,V,E,M], OutputStream) => Any,
                      Class[_ <: VertexProgramLike[I,V,E,M]],
                      VertexPartitioner) => Worker[I,V,E,M]) : BuilderType = {
    _workerFactory = Option(workerFactory)
    this
  }

  def vertexPartitioner(partitioner : VertexPartitioner) : BuilderType = {
    _partitioner = Option(partitioner)
    this
  }

  def build() : JoyGraphTest[I,V,E,M] = {
    val graphTestInstance = new JoyGraphTest[I,V,E,M](programClazz)

    _dataPath match {
      case Some(dataPath) => graphTestInstance.dataPath(dataPath)
      case None => throw new IllegalArgumentException("Missing path")
    }

    _workers match {
      case Some(workers) => graphTestInstance.numWorkers(workers)
      case None => throw new IllegalArgumentException("Missing number of workers")
    }

    _parser match {
      case Some(parser) => graphTestInstance.parser(parser)
      case None => throw new IllegalArgumentException("Missing line parser")
    }

    _writer match {
      case Some(writer) => graphTestInstance.writer(writer)
      case None => throw new IllegalArgumentException("Missing writer")
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

    _programParameters match {
      case Some(programParameters) => graphTestInstance.programParameters(programParameters)
      case None => // emit some warning
    }

    _outputPath match {
      case Some(outputPath) => graphTestInstance.outputPath(outputPath)
      case None => throw new IllegalArgumentException("Missing output path")
    }

    graphTestInstance
  }
}

protected[this] class JoyGraphTest[I,V,E,M](programClazz : Class[_ <: VertexProgramLike[I,V,E,M]]) {
  private[this] type Type = JoyGraphTest[I,V,E,M]
  private[this] var _workers : Int = _
  private[this] var _dataPath : String = _
  private[this] var _parser : (String) => (I,I,E) = _
  private[this] var _writer : (Vertex[I,V,E,M], OutputStream) => Any = _
  private[this] var _workerFactory : (Config, (String) => (I, I, E), (Vertex[I,V,E,M], OutputStream) => Any, Class[_ <: VertexProgramLike[I,V,E,M]], VertexPartitioner) => Worker[I,V,E,M] = _
  private[this] var _masterFactory : (Config, Cluster) => Master = _
  private[this] var _partitioner : VertexPartitioner = _
  private[this] var _programParameters : Option[(String, String)] = None
  private[this] var _outputPath : String = _

  def programParameters(keyValue : (String, String)) : Type = {
    _programParameters = Option(keyValue)
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

  def parser(f : (String) => (I, I, E)) : Type = {
    _parser = f
    this
  }

  def writer(f : (Vertex[I,V,E,M], OutputStream) => Any) : Type = {
    _writer = f
    this
  }

  def masterFactory(masterFactory : (Config, Cluster) => Master)  : Type = {
    _masterFactory = masterFactory
    this
  }

  def workerFactory(workerFactory :
                    (Config,
                      (String) => (I, I, E),
                      (Vertex[I,V,E,M], OutputStream) => Any,
                      Class[_ <: VertexProgramLike[I,V,E,M]],
                      VertexPartitioner) => Worker[I,V,E,M]) : Type = {
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
        workers.initial = ${_workers}
        data.path = "file://${_dataPath}"
        output.path = "file://${_outputPath}"
      }
      fs.defaultFS = "file:///"
      worker {
        suffix = "worker"
        output.lineWriterClass = "nl.joygraph.impl.hadoop.writer.HadoopLineWriter"
        input.lineProviderClass = "nl.joygraph.impl.hadoop.reader.HadoopLineProvider"
      }
      master.suffix = "master"
    """ + (_programParameters match {
        case Some(kv) => s"""${kv._1} = ${kv._2}\n"""
        case None =>
      })

    val jobConfig = ConfigFactory.parseString(jobCfg)

    _partitioner match {
      case hashPartitioner : VertexHashPartitioner => hashPartitioner.numWorkers(_workers)
      case _ => // noop
    }

    val seedPort = PortFinder.findFreePort(2552)
    var port = seedPort
    (0 until _workers).map { _ =>
      val config = ConfigFactory.parseString(cfg(port, seedPort))
      val system = ActorSystem(actorSystemName, config)
      system.actorOf(Props(classOf[BaseActor], jobConfig, _masterFactory, () => _workerFactory(jobConfig, _parser, _writer, programClazz, _partitioner)))
      port = PortFinder.findFreePort(port + 1)
      system.whenTerminated
    }.foreach(Await.ready(_, Duration(Int.MaxValue, TimeUnit.MILLISECONDS)))
  }
}
