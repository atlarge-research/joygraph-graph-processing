package io.joygraph.core.runner

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.actor.metrics.GeneralMetricsCollector
import io.joygraph.core.actor.{BaseActor, Master, Worker, WorkerProvider}
import io.joygraph.core.message.elasticity.WorkersResponse
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.program.ProgramDefinition
import io.joygraph.core.util.net.PortFinder

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

object JoyGraphLocalInstanceBuilder {
  def apply[I,V,E](programDefinition: ProgramDefinition[String, I,V,E]) : JoyGraphLocalInstanceBuilder[I,V,E] = {
    new JoyGraphLocalInstanceBuilder[I,V,E](programDefinition)
  }
}

class JoyGraphLocalInstanceBuilder[I,V,E](programDefinition: ProgramDefinition[String, I,V,E]) {

  protected[this] type BuilderType = JoyGraphLocalInstanceBuilder[I,V,E]
  protected[this] var _workers : Option[Int] = None
  protected[this] var _dataPath : Option[String] = None
  protected[this] var _vertexPath : Option[String] = None
  protected[this] var _workerFactory : Option[(Config, ProgramDefinition[String, _,_,_], VertexPartitioner) => Worker[_,_,_]] = None
  protected[this] var _masterFactory : Option[(Config, Cluster) => Master] = None
  protected[this] var _partitioner : Option[VertexPartitioner] = None
  protected[this] var _programParameters : ArrayBuffer[(String,String)] = ArrayBuffer.empty
  protected[this] var _outputPath : Option[String] = None
  protected[this] var _isElastic : Boolean = false
  protected[this] var _isDirected : Option[Boolean] = None
  protected[this] var _workerCores : Option[Int] = None
  protected[this] var _policy : Option[ElasticPolicy] = None

  def workerCores(workerCores : Int) : BuilderType = {
    _workerCores = Option(workerCores)
    this
  }

  def directed() : BuilderType = {
    _isDirected = Some(true)
    this
  }

  def undirected() : BuilderType = {
    _isDirected = Some(false)
    this
  }

  def policy(policy : ElasticPolicy) : BuilderType = {
    _policy = Some(policy)
    this
  }

  def elastic() : BuilderType = {
    _isElastic = true
    this
  }

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

  def vertexPath(path : String) : BuilderType = {
    _vertexPath = Option(path)
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

    _vertexPath match {
      case Some(vertexPath) => graphTestInstance.vertexPath(vertexPath)
      case None => // noop
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

    _isDirected match {
      case Some(directed) => graphTestInstance.directed(directed)
      case None => throw new IllegalArgumentException("Directness of graph is missing")
    }

    _workerCores match {
      case Some(workerCores) => graphTestInstance.workerCores(workerCores)
      case None => graphTestInstance.workerCores(Runtime.getRuntime.availableProcessors() - 1)
    }

    _policy match {
      case Some(policy) =>
        graphTestInstance.policy(policy)
      case None =>
        graphTestInstance.policy(ElasticPolicy.default())
    }

    graphTestInstance.setElastic(_isElastic)

    graphTestInstance
  }
}

protected[this] class JoyGraphLocalInstance(programDefinition : ProgramDefinition[String, _,_,_]) {
  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val actorSystemName = "JoyGraphTest"


  protected[this] type Type = JoyGraphLocalInstance

  protected[this] var _workerCores : Int = _
  protected[this] var _workers : Int = _
  protected[this] var _vertexPath : String = _
  protected[this] var _dataPath : String = _
  protected[this] var _workerFactory : (Config, ProgramDefinition[String, _,_,_], VertexPartitioner) => Worker[_,_,_] = _
  protected[this] var _masterFactory : (Config, Cluster) => Master = _
  protected[this] var _partitioner : VertexPartitioner = _
  protected[this] var _programParameters : ArrayBuffer[(String, String)] = _
  protected[this] var _outputPath : String = _
  protected[this] var _isDirected : Boolean = _

  // elasticity
  private[this] var _isElastic = false
  private[this] var _policy : ElasticPolicy = _
  private[this] val finishedLock = new CountDownLatch(1)
  private[this] val finishedCounter = new AtomicInteger(0)
  private[this] def finish(): Unit = {
    if (finishedCounter.decrementAndGet() == 0) {
      finishedLock.countDown()
    }
  }
  private[this] def addSystem() = {
    finishedCounter.incrementAndGet()
  }
  private[this] def waitForFinish() : Unit = {
    finishedLock.await()
  }

  private[this] val _workerProvider : () => WorkerProvider = () => new WorkerProvider {
    override def response(jobConf: Config, numWorkers: Int): Future[WorkersResponse] = {
      (0 until numWorkers).foreach{ _ =>
        val port = PortFinder.findFreePort()
        val jobConfig =
            ConfigFactory.parseString(createAkkaRemoteConfig(port))
              .withFallback(jobConf) // jobconf already contains the seed port of the cluster
        val system = ActorSystem(actorSystemName, jobConfig)
        system.actorOf(Props(classOf[BaseActor], jobConfig, _masterFactory, () => _workerFactory(jobConfig, programDefinition, _partitioner), None))
        addSystem()
        system.whenTerminated.foreach(_ =>
          finish()
        )
      }
      // immediately fulfill promise
      val promise = Promise[WorkersResponse]
      promise.success(WorkersResponse(numWorkers))
      promise.future
    }
  }

  private[this] val workerProviderSystem = ActorSystem(actorSystemName)
  private[this] val _workerProviderRef = workerProviderSystem.actorOf(Props(_workerProvider()))

  def directed(directed: Boolean) = _isDirected = directed

  def setElastic(isElastic: Boolean) = {
    _isElastic = isElastic
  }

  def programParameters(programParameters : ArrayBuffer[(String, String)]) : Type = {
    _programParameters = programParameters
    this
  }


  def workerCores(workerCores: Int) : Type = {
    _workerCores = workerCores
    this
  }

  def numWorkers(workers : Int): Type = {
    _workers = workers
    this
  }

  def vertexPath(vertexPath: String) : Type = {
    _vertexPath = vertexPath
    this
  }

  def policy(policy: ElasticPolicy) : Type = {
    _policy = policy
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

  private[this] def createAkkaRemoteConfig(port : Int) = {
    s"""
       |akka {
       |      remote {
       |        watch-failure-detector.acceptable-heartbeat-pause = 10
       |        netty.tcp {
       |            hostname = "127.0.0.1"
       |            maximum-frame-size = 10M
       |            port = $port
       |        }
       |      }
       |}
     """.stripMargin
  }

  private[this] def createAkkaClusterConfig(actorSystemName : String, seedPort : Int) : String = {
    s"""
  akka.actor.warn-about-java-serializer-usage = false
  akka {
      actor {
        provider = "akka.cluster.ClusterActorRefProvider"
      }
      cluster {
        seed-nodes = [
            "akka.tcp://$actorSystemName@127.0.0.1:$seedPort"
          ]
        auto-down = on
      }
  }
  akka.extensions = [ "akka.cluster.metrics.ClusterMetricsExtension" ]
  akka.cluster.metrics.enabled = off
  akka.cluster.metrics.collector.provider = ${classOf[GeneralMetricsCollector].getName}
  akka.cluster.metrics.collector.fallback = false
  akka.cluster.metrics.collector.sample-interval = 0.5s
  akka.cluster.metrics.collector.gossip-interval = 0.5s
      """
  }

  private[this] def createJobConfig() : Config = {
    val vertexPathKeyValue : String = Option[String](_vertexPath) match {
      case Some(vertexPath) =>
        s"""
           |job.vertices.path = "file://${_vertexPath}"
             """.stripMargin
      case None =>
        ""
    }

    val jobCfg =
      s"""
      job {
        program.definition.class = "${programDefinition.getClass.getName}"
        master.memory = 1000
        master.cores = 1
        worker.memory = 2000
        worker.cores = ${_workerCores}
        workers.initial = ${_workers}
        data.path = "file://${_dataPath}"
        output.path = "file://${_outputPath}"
        directed = ${_isDirected}
        policy.class = "${_policy.getClass.getName}"
      }
      worker {
        suffix = "worker"
        output.lineWriterClass = "io.joygraph.impl.hadoop.writer.HadoopLineWriter"
        input.lineProviderClass = "io.joygraph.impl.hadoop.reader.HadoopLineProvider"
      }
      master.suffix = "master"
    """ + vertexPathKeyValue + "\n" +
        {
          if (_programParameters.isEmpty)
            "\n"
          else
            _programParameters.map(kv => s"""${kv._1} = ${kv._2}\n""").reduce(_ + "" + _)
        }
    ConfigFactory.parseString(jobCfg)
  }

  def workerProviderRef = {
    this._workerProviderRef
  }

  def run(): Unit = {
    val masterFac = if (_isElastic) {
      (config : Config, cluster : Cluster) => {
        val master = _masterFactory(config, cluster)
        master.setWorkerProvider(workerProviderRef)
        master
      }
    } else {
      _masterFactory
    }

    val pureJobConfig = createJobConfig()
    val seedPort = PortFinder.findFreePort(2552)
    var port = seedPort
    (0 until (_workers + 1)).foreach { _ =>
      val config =
        ConfigFactory.parseString(createAkkaClusterConfig(actorSystemName, seedPort))
          .withFallback(ConfigFactory.parseString(createAkkaRemoteConfig(port)))
          .withFallback(pureJobConfig)
      val system = ActorSystem(actorSystemName, config)
      system.actorOf(Props(classOf[BaseActor], config, masterFac, () => _workerFactory(pureJobConfig, programDefinition, _partitioner), None))
      port = PortFinder.findFreePort(port + 1)

      // wait for system to terminate
      addSystem()
      system.whenTerminated.foreach(_ =>
        finish()
      )
    }

    waitForFinish()
    Await.ready(workerProviderSystem.terminate(), Duration(Int.MaxValue, TimeUnit.MILLISECONDS) )
  }
}
