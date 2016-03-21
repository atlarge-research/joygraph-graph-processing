package io.joygraph.impl.hadoop.actor

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import io.joygraph.core.actor.{BaseActor, Master, Worker}
import io.joygraph.core.config.JobSettings
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

    val clusterNodeConf =  ConfigFactory.parseString(
      s"""
         |akka {
         |  cluster {
         |    seed-nodes = [
         |      "akka.tcp://$actorSystemName@$hostName:$port"
         |    ]
         |    auto-down = on
         |  }
         |}
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
         Worker.workerWithSerializedTrieMapMessageStore(masterConf, definition, new VertexHashPartitioner)
      }
    ))
    // wait indefinitely until it terminates
    Await.ready(system.whenTerminated, Duration(Int.MaxValue, TimeUnit.MILLISECONDS))
    println("I have terminated")
  }
}

class YarnAMBaseActor(paths : String, jobConf : Config, workerConf : Config, masterFactory : (Config, Cluster) => _ <: Master, workerFactory : () => Worker[_,_,_]) extends BaseActor(jobConf, masterFactory, workerFactory) {

  private[this] val dfsPaths = YARNUtils.deserializeResourcesFileStatuses(paths)
  private[this] val jarFileStatus = dfsPaths.head
  private[this] val workerMemory = JobSettings(jobConf).workerMemory
  private[this] val launcherExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1, new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r, "container-launcher")
  }))

  // assume the Hdfs configuration is retrieved from classpath
  private[this] val hdfsConfiguration: Configuration = new HdfsConfiguration()
  private[this] val fs = FileSystem.get(hdfsConfiguration)

  val allocListener : AMRMClientAsync.CallbackHandler = new AMRMClientAsync.CallbackHandler {
    override def onError(e: Throwable): Unit = log.error(e, "container allocation error")

    override def getProgress: Float = 0f

    override def onShutdownRequest(): Unit = log.info("Received shutdown request")

    override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = {
      // noop
    }

    override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = {
      statuses.foreach(status => s"${status.getContainerId} completed")
      // noop
    }

    override def onContainersAllocated(containers: util.List[Container]): Unit = {

      implicit val executionContext = launcherExecutionContext

      containers.par.foreach { container =>

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
        commands += YARNUtils.workerCommand(jarFileName, confFileName, workerMemory)

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

  val conf = new YarnConfiguration()
  //    conf.addResource() // TODO get configuration from classpath?


  val amRMClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](1000, allocListener)
  amRMClient.init(conf)
  amRMClient.start()

  val containerListener = new NMClientAsync.CallbackHandler {
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

  val nmClientAsync = new NMClientAsyncImpl(containerListener)
  nmClientAsync.init(conf)
  nmClientAsync.start()

  val amMemory = JobSettings(jobConf).masterMemory
  val amVCores = JobSettings(jobConf).masterCores

  val capability = Resource.newInstance(amMemory, amVCores)
  val priority = Priority.UNDEFINED // TODO set in conf?

  val appMasterHostname = NetUtils.getHostName
  val appMasterRpcPort = PortFinder.findFreePort()
  val appMasterTrackingUrl = null
  val response = amRMClient
    .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
      appMasterTrackingUrl)

  // check job.workers.initial
  val numWorkers = JobSettings(jobConf).initialNumberOfWorkers

  for (i <- 1 to numWorkers) {
    amRMClient.addContainerRequest(new AMRMClient.ContainerRequest(capability, null, null, priority, true))
  }

  override def postStop(): Unit = {
    super.postStop()
    // TODO propagate fail/success state
    val appStatus = FinalApplicationStatus.SUCCEEDED
    amRMClient.unregisterApplicationMaster(appStatus, "Shut down successfully", null)
    amRMClient.stop()
    nmClientAsync.stop()
  }
}