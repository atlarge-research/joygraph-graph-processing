package io.joygraph.impl.hadoop.actor

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import io.joygraph.core.actor.metrics.GeneralMetricsCollector
import io.joygraph.core.actor.{BaseActor, Master, Worker}
import io.joygraph.core.config.JobSettings
import io.joygraph.core.message.elasticity.{RegisterWorkerProvider, WorkersRequest}
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import io.joygraph.core.program.ProgramDefinition
import io.joygraph.core.util.net.{NetUtils, PortFinder}
import io.joygraph.impl.hadoop.util.YARNUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.reflect.io.File

object YarnAMBaseActor {
  def main(args : Array[String]) : Unit = {
    val configLocation : String = args(0)
    val paths : String = args(1)
    val submissionClientActorAddress = if (args.length > 2) Some(args(2)) else None

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

    val clusterNodeConf =  ConfigFactory.parseString(
      s"""
         |akka {
         |  cluster {
         |    seed-nodes = [
         |      "akka.tcp://$actorSystemName@$hostName:$port"
         |    ]
         |    auto-down = on
         |    failure-detector {
         |      acceptable-heartbeat-pause = 120 s
         |      threshold = 24
         |      min-std-deviation = 30 s
         |    }
         |    use-dispatcher = cluster-dispatcher
         |  }
         |}
         |
         |cluster-dispatcher {
         |  type = "Dispatcher"
         |  executor = "fork-join-executor"
         |  fork-join-executor {
         |    parallelism-min = 2
         |    parallelism-max = 4
         |  }
         |}
         |akka.extensions = [ "akka.cluster.metrics.ClusterMetricsExtension" ]
         |akka.cluster.metrics.enabled = off
         |akka.cluster.metrics.collector.provider = ${classOf[GeneralMetricsCollector].getName}
         |akka.cluster.metrics.collector.fallback = false
       """.stripMargin)

    // this is the configuration that the workers will get.
    // as workers need to know the seed-nodes
    // this actorsystem is going to serve as a seed node.
    val workerConf = clusterNodeConf
      .withFallback(jobConf)

    val masterConf = remotingConf
      .withFallback(clusterNodeConf)
      .withFallback(jobConf)

    val definition : ProgramDefinition[String,_,_,_] = Class.forName(JobSettings(jobConf).programDefinition).newInstance().asInstanceOf[ProgramDefinition[String,_,_,_]]
    val system = ActorSystem(actorSystemName, masterConf)
    system.actorOf(Props(classOf[YarnAMBaseActor], paths, masterConf, workerConf,
      (conf : Config, cluster : Cluster) => {
        val master = new Master(conf, cluster) with HadoopMaster
        Master.initialize(master)
      }, () => {
//         Worker.workerWithSerializedTrieMapMessageStore(masterConf, definition, new VertexHashPartitioner)
         Worker.workerWithSerializeOpenHashMapStore(masterConf, definition, new VertexHashPartitioner)
      },
      submissionClientActorAddress
    ))
    // wait indefinitely until it terminates
    Await.ready(system.whenTerminated, Duration(Int.MaxValue, TimeUnit.MILLISECONDS))
    println("I have terminated")
  }
}

class YarnAMBaseActor
(paths : String,
 jobConf : Config,
 workerConf : Config,
 masterFactory : (Config, Cluster) => _ <: Master,
 workerFactory : () => Worker[_,_,_],
 submissionClientActorAddress : Option[String]) extends BaseActor(jobConf, masterFactory, workerFactory, submissionClientActorAddress) {

  private[this] val dfsPaths = YARNUtils.deserializeResourcesFileStatuses(paths)
  private[this] val jarFileStatus = dfsPaths.head
  private[this] val launcherExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1, new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r, "container-launcher")
  }))

  // assume the Hdfs configuration is retrieved from classpath
  private[this] val hdfsConfiguration: Configuration = new HdfsConfiguration()
  private[this] val fs = FileSystem.get(hdfsConfiguration)

  private[this] val jobSettings = JobSettings(jobConf)

  // check job.workers.initial
  private[this] val numWorkers = jobSettings.initialNumberOfWorkers
  private[this] val workerMemory = jobSettings.workerMemory
  private[this] val workerCores = jobSettings.workerCores
  private[this] var capability : Resource = _
  // TODO non-homogeneous priorities have a side-effect in YARN
  private[this] val currentPriority = new AtomicInteger(1)

  private[this] val allocListener : AMRMCallBackHandler = new AMRMCallBackHandler

  private[this] val conf = new YarnConfiguration()
  //    conf.addResource() // TODO get configuration from classpath?

  private[this] val amRMClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](1000, allocListener)

  private[this] var finalApplicationStatus = FinalApplicationStatus.SUCCEEDED

  private[this] val containerListener = new NMClientAsync.CallbackHandler {
    override def onContainerStarted(containerId: ContainerId, allServiceResponse: util.Map[String, ByteBuffer]): Unit =
      log.info(s"Container $containerId started")

    override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit =
      log.info(s"Container status $containerId: $containerStatus")

    override def onContainerStopped(containerId: ContainerId): Unit =
      log.info(s"Container $containerId stopped")

    override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit =
      log.info(s"Container $containerId start error: $t")

    override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit =
      log.info(s"Container $containerId stop error: $t")

    override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit =
      log.info(s"Container $containerId status error: $t")
  }

  private[this] val nmClientAsync = new NMClientAsyncImpl(containerListener)

  private[this] def addContainerRequest(): Unit = {
    log.info("Requesting container with {} memory and {} cores", capability.getMemory, capability.getVirtualCores)
    amRMClient.addContainerRequest(new AMRMClient.ContainerRequest(capability, null, null, YARNUtils.newPriority(currentPriority.incrementAndGet()), true))
  }

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting AMRMClient")
    amRMClient.init(conf)
    amRMClient.start()
    log.info("Starting NMClient")
    nmClientAsync.init(conf)
    nmClientAsync.start()

    val appMasterHostname = NetUtils.getHostName
    val appMasterRpcPort = PortFinder.findFreePort()
    val appMasterTrackingUrl = null
    log.info("Registering application master")
    val response = amRMClient
      .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
        appMasterTrackingUrl)
    capability = YARNUtils.cappedResource(response.getMaximumResourceCapability, workerMemory, workerCores)

    log.info("Requesting {} containers", numWorkers)
    allocListener.setContainersExpected(numWorkers)
    // request containers
    for (i <- 1 to numWorkers) {
      addContainerRequest()
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    // TODO propagate fail/success state

    // TODO kill all containers
    amRMClient.unregisterApplicationMaster(finalApplicationStatus, "Shut down successfully", null)
    amRMClient.stop()
    nmClientAsync.stop()
  }

  override def spawnMaster(): Unit = {
    super.spawnMaster()
    masterRef.get ! RegisterWorkerProvider(self)
  }

  override def receive: PartialFunction[Any, Unit] = (({
    case WorkersRequest(jobConf, numExtraWorkers) =>
      log.info("Requesting {} additional containers", numExtraWorkers)
      allocListener.setContainersExpected(numExtraWorkers)
      // request containers
      for (i <- 1 to numExtraWorkers) {
        addContainerRequest()
      }
  } : PartialFunction[Any, Unit]) :: super.receive :: Nil).reduceLeft(_ orElse _)

  class AMRMCallBackHandler extends AMRMClientAsync.CallbackHandler {
    private[this] var allocatedContainers : AtomicInteger = _
    private[this] var containersRequested : Int = _

    def setContainersExpected(n : Int) = {
      containersRequested = n
      allocatedContainers = new AtomicInteger(0)
    }

    override def onError(e: Throwable): Unit = log.error(e, "container allocation error")

    override def getProgress: Float = 0f

    override def onShutdownRequest(): Unit = log.info("Received shutdown request")

    override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = {
      // noop
    }

    override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = {
      statuses.foreach {
        status =>
          log.info(s"${status.getContainerId} completed\n" +
            s"diagnostics: ${status.getDiagnostics}\n" +
            s"exit status: ${status.getExitStatus}")
          if (status.getExitStatus == 255) {
            log.info("Abnormal exit status, terminating job, job failed")
            finalApplicationStatus = FinalApplicationStatus.FAILED
            context.system.terminate()
          }
      }
      // noop
    }

    override def onContainersAllocated(containers: util.List[Container]): Unit = synchronized {
      log.info("Received {} containers", containers.size())
      val currentlyAllocatedContainers = allocatedContainers.addAndGet(containers.size())
      log.info("Currently allocated {} containers", currentlyAllocatedContainers)
      val containersToUse = containers

      if (currentlyAllocatedContainers < containersRequested) {
        log.info("Did not receive enough containers : {}/{}", currentlyAllocatedContainers, containersRequested)
        val numToRequest = containersRequested - currentlyAllocatedContainers
        log.info("Requesting missing {} containers", numToRequest)
        (1 to numToRequest).foreach(_ => addContainerRequest())
      }

      if (currentlyAllocatedContainers > containersRequested) {
        val diff = currentlyAllocatedContainers - containersRequested
        allocatedContainers.addAndGet(-diff)
        log.info("Releasing {} excess containers", diff)
        // release excess containers
        var i = 0
        while (i < diff) {
          val containerToRelease = containers.last
          containersToUse.remove(containerToRelease)
          amRMClient.releaseAssignedContainer(containerToRelease.getId)
          i += 1
        }
      }

      log.info("Launching {} containers", containersToUse.size)
      containersToUse.foreach { container =>
        val localResources : util.Map[String, LocalResource] = new util.HashMap[String, LocalResource]
        val commands : util.List[String] = new util.ArrayList[String]()
        val env : util.Map[String, String] = new util.HashMap[String, String]()

        val jarFileName = jarFileStatus.getPath.getName
        val configFile = File.makeTemp("worker", ".conf")
        configFile.writeAll(workerConf.root().render())
        val confFileName = configFile.name

        val (_, jarResource) = YARNUtils.localResource(jarFileName, jarFileStatus)
        val (_, confResource) = YARNUtils.localResource(fs, confFileName, configFile.toAbsolute.toString(), System.currentTimeMillis() + "")

        localResources += jarResource
        localResources += confResource
        env += YARNUtils.classPath()
        log.info(s"Launching worker with ${workerMemory * 8 / 10}")
        commands += YARNUtils.workerCommand(jarFileName, confFileName, workerMemory * 8 / 10)

        val credentials: Credentials = UserGroupInformation.getCurrentUser.getCredentials
        val dob = new DataOutputBuffer()
        credentials.writeTokenStorageToStream(dob)
        val fsTokens = ByteBuffer.wrap(dob.getData, 0, dob.getLength)

        // we run the actorsystem
        val ctx = ContainerLaunchContext.newInstance(
          localResources, env, commands, null, fsTokens, null)

        nmClientAsync.startContainerAsync(container, ctx)
      }
    }
  }
}