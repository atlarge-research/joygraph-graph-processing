package nl.joygraph.core.actor

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor.{Actor, ActorLogging, ActorSelection}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import nl.joygraph.core.config.JobSettings
import nl.joygraph.core.message._
import nl.joygraph.core.util.FutureUtil

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

object Master {
  def create(clazz : Class[_ <: Master], conf : Config, cluster : Cluster) : Master = {
    // assumes one constructor
    val master = clazz.getConstructor(classOf[Config], classOf[Cluster]).newInstance(conf, cluster)
    master.initialize()
    master
  }
}

abstract class Master protected(conf : Config, cluster : Cluster) extends Actor with ActorLogging {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val timeout = Timeout(30, TimeUnit.SECONDS)

  val jobSettings : JobSettings = JobSettings(conf)
  var addressWorkerIdMap : mutable.Map[String, Int] = mutable.Map.empty
  var workerIdAddressMap : ArrayBuffer[String] = ArrayBuffer.fill(jobSettings.initialNumberOfWorkers)("")
  var successReceived : AtomicInteger = _

  private[this] var initialized: Boolean = false
  val numVertices = new AtomicLong(0)
  val numEdges = new AtomicLong(0)

  def initialize() : Unit = synchronized {
    if (cluster.state.members.size < jobSettings.initialNumberOfWorkers) {
      return
    }
    if (initialized) {
      return
    }

    // wait for all members to be up
    log.info("Initializing")
    initialized = true

    // we are up
    var workerId = 0

    cluster.state.members.foreach { x =>
      val akkaPath = x.address.toString + "/user/" + jobSettings.workerSuffix
      // give each member an id
      context.actorSelection(akkaPath) ! WorkerId(workerId)
      // TODO get reply

      addressWorkerIdMap.put(akkaPath, workerId)
      workerIdAddressMap(workerId) = akkaPath

      workerId += 1
    }

    val masterPath = cluster.selfAddress.toString + "/user/" + jobSettings.masterSuffix
    val actorSelections: ArrayBuffer[ActorSelection] = allWorkers()

    FutureUtil.callbackOnAllComplete(sendMasterAddress(actorSelections, masterPath)) {
      FutureUtil.callbackOnAllComplete(sendMapping(actorSelections)) {
        sendPaths(actorSelections, jobSettings.dataPath)
      }
    }
  }

  def allWorkers() = workerIdAddressMap.map(context.actorSelection)

  def sendMasterAddress(actorSelections : Iterable[ActorSelection], masterPath : String) = {
    log.info("Sending master address")
    actorSelections.map(x => (x ? MasterAddress(masterPath)).mapTo[Boolean])
  }

  protected[this] def split(workerId : Int, totalNumNodes : Int, path : String) : (Long, Long)

  def sendPaths(actorSelection : Iterable[ActorSelection], dataPath : String) = {
    successReceived = new AtomicInteger(actorSelection.size)
    actorSelection
      .zipWithIndex.foreach {
      case (actor, index) =>
        val (position, length) = split(index, cluster.state.members.size, dataPath)
        val response: Future[Boolean] = (actor ? new PrepareLoadData()).mapTo[Boolean]
        response.foreach(_ => actor ! LoadData(dataPath, position, length))
    }
  }

  def sendMapping(actorSelections : Iterable[ActorSelection]): Iterable[Future[Boolean]] = {
    log.info("Sending address mapping")
    actorSelections.map(x => (x ? WorkerMap(workerIdAddressMap)).mapTo[Boolean])
  }

  override def receive = {
    case MemberUp(member) =>
      initialize()
    case LoadingComplete(workerId, numV, numEdge) =>
      println(s"$workerId $numV $numEdge done")
      println(s"total : ${numVertices.addAndGet(numV)} ${numEdges.addAndGet(numEdge)}")
    case AllLoadingComplete() =>
      if (successReceived.decrementAndGet() == 0) {
        allWorkers().foreach( _ ! AllLoadingComplete())
        successReceived = null
      }
  }

}
