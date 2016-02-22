package nl.joygraph.core.actor

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import nl.joygraph.core.actor.state.GlobalState
import nl.joygraph.core.config.JobSettings
import nl.joygraph.core.message._
import nl.joygraph.core.message.superstep.{DoNextStep, PrepareSuperStep, RunSuperStep, SuperStepComplete}
import nl.joygraph.core.util.FutureUtil

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
  var workerIdAddressMap : ArrayBuffer[ActorRef] = ArrayBuffer.fill(jobSettings.initialNumberOfWorkers)(null)
  var successReceived : AtomicInteger = _

  private[this] var initialized: Boolean = false
  val numVertices = new AtomicLong(0)
  val numEdges = new AtomicLong(0)

  var currentSuperStep = 0

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

    FutureUtil.callbackOnAllComplete(cluster.state.members.map { x =>
      log.info("Retrieving ActorRefs")
      val akkaPath = x.address.toString + "/user/" + jobSettings.workerSuffix
      // give each member an id
      val fActorRef: Future[ActorRef] = (context.actorSelection(akkaPath) ? WorkerId(workerId)).mapTo[ActorRef]
      val currentWorkerId = workerId
      fActorRef.foreach(x => workerIdAddressMap(currentWorkerId) = x)
      workerId += 1
      fActorRef
    }) {
      log.info("Distributing ActorRefs")
      val masterPath = cluster.selfAddress.toString + "/user/" + jobSettings.masterSuffix
      val actorSelections: ArrayBuffer[ActorRef] = allWorkers()

      // TODO sometimes sendMasteraddress does not complete, probably something with the callbackonallcomplete.
      // the latch probably only gets triggered on the moment the future is completed and not when the future has already been completed.
      FutureUtil.callbackOnAllComplete(sendMasterAddress(actorSelections, masterPath)) {
        FutureUtil.callbackOnAllComplete(sendMapping(actorSelections)) {
          sendPaths(actorSelections, jobSettings.dataPath)
        }
      }
    }

  }

  def allWorkers() = workerIdAddressMap

  def sendMasterAddress(actorRefs : Iterable[ActorRef], masterPath : String) = {
    log.info("Sending master address")
    actorRefs.map(x => (x ? MasterAddress(self)).mapTo[Boolean])
  }

  protected[this] def split(workerId : Int, totalNumNodes : Int, path : String) : (Long, Long)

  def sendPaths(actorRefs : Iterable[ActorRef], dataPath : String) = {
    successReceived = new AtomicInteger(actorRefs.size)
    // set the state
    FutureUtil.callbackOnAllComplete(actorRefs.map(x => (x ? State(GlobalState.LOAD_DATA)).mapTo[Boolean])) {
      log.info("State set to LOAD_DATA")
      log.info("Sending PrepareLoadData")
      FutureUtil.callbackOnAllComplete(actorRefs.map(x => (x ? PrepareLoadData()).mapTo[Boolean])) {
        actorRefs.zipWithIndex.foreach {
          case (actor, index) =>
            val (position, length) = split(index, cluster.state.members.size, dataPath)
            actor ! LoadData(dataPath, position, length)
        }
      }
    }
  }

  def sendMapping(actorRefs : Iterable[ActorRef]): Iterable[Future[Boolean]] = {
    log.info("Sending address mapping")
    actorRefs.map(x => (x ? WorkerMap(workerIdAddressMap)).mapTo[Boolean])
  }

  def sendSuperStepState(): Unit = {
    FutureUtil.callbackOnAllComplete(allWorkers().map(x => (x ? State(GlobalState.SUPERSTEP)).mapTo[Boolean])) {
      log.info("Sending prepare superstep")
      FutureUtil.callbackOnAllComplete(allWorkers().map(x => (x ? PrepareSuperStep()).mapTo[Boolean])) {
        log.info("Sending runsuperstep")
        successReceived = new AtomicInteger(allWorkers().size)
        allWorkers().foreach(_ ! RunSuperStep(currentSuperStep))
      }
    }
  }

  override def receive = {
    case MemberUp(member) =>
      initialize()
    case LoadingComplete(workerId, numV, numEdge) =>
      println(s"$workerId $numV $numEdge done")
      println(s"total : ${numVertices.addAndGet(numV)} ${numEdges.addAndGet(numEdge)}")
    case AllLoadingComplete() =>
      if (successReceived.decrementAndGet() == 0) {
        successReceived = null
        allWorkers().foreach(_ ! AllLoadingComplete())
        // set to superstep
        sendSuperStepState()
      }

    case SuperStepComplete() =>
      log.info(s"Left : ${successReceived.get()}")
      if (successReceived.decrementAndGet() == 0) {
        currentSuperStep += 1
        successReceived = new AtomicInteger(allWorkers().size)
        FutureUtil.callbackOnAllCompleteWithResults(allWorkers().map(x => (x ? SuperStepComplete()).mapTo[DoNextStep])) {
          implicit results =>
            log.info("Do next step ?")
            val doNextStep = results.map(_.yes).reduce(_ && _)
            log.info(s"${results.size}")
            log.info(s"${results.map(_.toString).reduce(_ + " " + _)}")
            log.info(s"Hell ${doNextStep}")
            // set to superstep
            if (doNextStep) {
              allWorkers().foreach(_ ! RunSuperStep(currentSuperStep))
            } else {
              println(s"we're done, fuckers at ${currentSuperStep - 1}")
            }
        }
      }
  }

}
