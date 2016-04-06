package io.joygraph.core.actor

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import io.joygraph.core.actor.state.GlobalState
import io.joygraph.core.config.JobSettings
import io.joygraph.core.message._
import io.joygraph.core.message.elasticity._
import io.joygraph.core.message.superstep._
import io.joygraph.core.program.Aggregator
import io.joygraph.core.util.{Errors, ExecutionContextUtil, FutureUtil}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object Master {
  def initialize(master : Master) : Master = {
    // assumes one constructor
    master.initialize()
    master
  }
}

abstract class Master(protected[this] val conf : Config, cluster : Cluster) extends Actor with ActorLogging {

  private[this] implicit val executionContext = ExecutionContext.fromExecutor(
    ExecutionContextUtil.createForkJoinPoolWithPrefix("master-akka-message-"),
    (t: Throwable) => {
      log.info("Terminating on exception" + Errors.messageAndStackTraceString(t))
      terminate()
    }
  )
  // TODO set in configuration
  implicit val timeout = Timeout(30, TimeUnit.SECONDS)

  val jobSettings : JobSettings = JobSettings(conf)
  var workerIdAddressMap : Map[Int, AddressPair] = Map.empty
  var successReceived : AtomicInteger = _
  var doneDoOutput : AtomicInteger = _
  var elasticityPromise : ElasticityPromise = _
  val initializationLock = new Semaphore(1)
  // TODO keep state in Master explicitly instead of implicitly
  var doingElasticity : Boolean = false


  // An actor providing actors
  protected[this] var workerProvider : Option[ActorRef] = None

  private[this] var initialized: Boolean = false
  private[this] var _aggregatorMapping : scala.collection.mutable.Map[String, Aggregator[_]] = mutable.OpenHashMap.empty
  val numVertices = new AtomicLong(0)
  val numEdges = new AtomicLong(0)

  var currentSuperStep = 0

  def workerMembers() = cluster.state.members.filterNot(_.address == cluster.selfAddress)

  def setWorkerProvider(workerProvider : ActorRef) = this.workerProvider = Option(workerProvider)

  def initialize() : Unit = {
    initializationLock.acquire()
    if (initialized) {
      initializationLock.release()
      return
    }

    // <= includes master
    if (cluster.state.members.size <= jobSettings.initialNumberOfWorkers) {
      initializationLock.release()
      return
    }

    // wait for all members to be up
    log.info("Initializing")
    // TODO cluster.state.members.size may not be always up to date, retrieve count from parent (BaseActor)
    log.info(s"There are ${cluster.state.members.size} members")

    // we are up
    var workerId = 0
    // cluster state members includes itself, have to exclude self.
    val workerIdAddressMapBuilder = TrieMap.empty[Int, AddressPair]
    FutureUtil.callbackOnAllComplete(workerMembers().map { x =>
      log.info("Retrieving ActorRefs")
      val akkaPath = x.address.toString + "/user/" + jobSettings.workerSuffix
      // give each member an id
      val fActorRef: Future[AddressPair] = (context.actorSelection(akkaPath) ? WorkerId(workerId)).mapTo[AddressPair]
      val currentWorkerId = workerId
      val assignedFuture: Future[Unit] = fActorRef.map(x => workerIdAddressMapBuilder(currentWorkerId) = x)
      workerId += 1
      assignedFuture
    }) {
      workerIdAddressMap = workerIdAddressMapBuilder.toMap
      log.info("Distributing ActorRefs")
      val actorSelections: Iterable[ActorRef] = allWorkers().map(_.actorRef)

      // TODO the worker sometimes sends a null reference as the actorRef field in AddressPair, this will result in a nullpointer exception
      FutureUtil.callbackOnAllComplete(sendMasterAddress(actorSelections)) {
        FutureUtil.callbackOnAllComplete(sendMapping(actorSelections)) {
          sendPaths(actorSelections, jobSettings.dataPath)
          initialized = true
          initializationLock.release()
        }
      }
    }
  }

  def allWorkers() = workerIdAddressMap.values

  def sendMasterAddress(actorRefs : Iterable[ActorRef]) : Iterable[Future[Boolean]] = {
    log.info("Sending master address")
    actorRefs.map(x => (x ? MasterAddress(self)).mapTo[Boolean])
  }

  protected[this] def split(workerId : Int, totalNumNodes : Int, path : String) : (Long, Long)
  protected[this] def mkdirs(path : String) : Boolean

  def sendPaths(actorRefs : Iterable[ActorRef], dataPath : String) = {
    log.info(s"Sending paths to ${actorRefs.size} workers")
    successReceived = new AtomicInteger(actorRefs.size)
    // set the state
    FutureUtil.callbackOnAllComplete(actorRefs.map(x => (x ? State(GlobalState.LOAD_DATA)).mapTo[Boolean])) {
      log.info("State set to LOAD_DATA")
      log.info("Sending PrepareLoadData")
      FutureUtil.callbackOnAllComplete(actorRefs.map(x => (x ? PrepareLoadData()).mapTo[Boolean])) {
        actorRefs.zipWithIndex.foreach {
          case (actor, index) =>
            val (position, length) = split(index, workerMembers().size, dataPath)
            jobSettings.verticesPath match {
              case Some(verticesPath) =>
                val (verticesPosition, verticesLength) = split(index, workerMembers().size, verticesPath)
                actor ! LoadDataWithVertices(verticesPath, verticesPosition, verticesLength, dataPath, position, length)
              case None =>
                actor ! LoadData(dataPath, position, length)
            }

        }
      }
    }
  }

  def sendMapping(actorRefs : Iterable[ActorRef]): Iterable[Future[Boolean]] = {
    log.info("Sending address mapping")
    val immutableWorkerMap = workerIdAddressMap.toMap
    actorRefs.map(x => (x ? WorkerMap(immutableWorkerMap)).mapTo[Boolean])
  }

  def sendSuperStepState(numVertices : Long, numEdges : Long): Unit = {
    FutureUtil.callbackOnAllComplete(allWorkers().map(x => (x.actorRef ? State(GlobalState.SUPERSTEP)).mapTo[Boolean])) {
      log.info("Sending prepare superstep")
      FutureUtil.callbackOnAllComplete(allWorkers().map(x => (x.actorRef ? PrepareSuperStep(numVertices, numEdges)).mapTo[Boolean])) {
        log.info("Sending runsuperstep")
        successReceived = new AtomicInteger(allWorkers().size)
        allWorkers().foreach(_.actorRef ! RunSuperStep(currentSuperStep))
      }
    }
  }

  def sendDoOutput() : Unit = {
    // TODO maybe create output dir up front
    mkdirs(jobSettings.outputPath)
    FutureUtil.callbackOnAllComplete(allWorkers().map(x => (x.actorRef ? State(GlobalState.POST_SUPERSTEP)).mapTo[Boolean])) {
      doneDoOutput = new AtomicInteger(allWorkers().size)
      allWorkers().zipWithIndex.foreach{case (worker, index) => worker.actorRef ! DoOutput(s"${jobSettings.outputPath}/$index.part")}
    }
  }

  def doElasticity(): Future[Map[Int,AddressPair]] = {
    elasticityPromise = new ElasticityPromise(workerIdAddressMap, timeout, executionContext)
    // request number
    workerProvider match {
      case Some(provider) =>
        provider ! WorkersRequest(conf, 1)
      case None =>
        // if none then we complete promise without changing the worker map
        elasticityPromise.success(workerIdAddressMap)
    }

    elasticityPromise.future
  }

  override def receive = {
    case InitiateTermination() =>
      terminate()
    case ElasticGrowComplete() => {
      elasticityPromise.elasticGrowCompleted()
    }
    case WorkersResponse(numWorkers) =>
      // todo set number of workers to expect
      elasticityPromise.numWorkersExpected(numWorkers)
    case NewWorker() =>
      val senderRef = sender()

      // TODO make this a method
      // assumes workerIdAddressMap is not empty!
      val nextWorkerId = workerIdAddressMap.keys.max + 1
      val fAddressPair = (senderRef ? WorkerId(nextWorkerId)).mapTo[AddressPair]
      fAddressPair.foreach{
        elasticityPromise.newWorker(nextWorkerId, _)
      }
    case MemberUp(member) =>
      if (!initialized) {
        initialize()
      } else if (doingElasticity) {
        // elasticity
        // send master address to new worker
        val akkaPath = member.address.toString + "/user/" + jobSettings.workerSuffix
        context.actorSelection(akkaPath) ! InitNewWorker(self, currentSuperStep)
      }

    case LoadingComplete(workerId, numV, numEdge) =>
      println(s"$workerId $numV $numEdge done")
      numVertices.addAndGet(numV)
      numEdges.addAndGet(numEdge)
      if (successReceived.decrementAndGet() == 0) {
        println(s"total : ${numVertices.get} ${numEdges.get}")
        // set to superstep
        sendSuperStepState(numVertices.get, numEdges.get)
      }
    case AllLoadingComplete() =>
      if (successReceived.decrementAndGet() == 0) {
        successReceived = new AtomicInteger(allWorkers().size)
        allWorkers().foreach(_.actorRef ! AllLoadingComplete())
      }
    case BarrierComplete() => {
      if (successReceived.decrementAndGet() == 0) {
        log.info("Barrier complete")
        //reset aggregator
        _aggregatorMapping.foreach{
          case (name, aggregator) =>
            _aggregatorMapping(name).masterPrepareStep()
        }
        successReceived = new AtomicInteger(allWorkers().size)
        allWorkers().foreach(_.actorRef ! RunSuperStep(currentSuperStep))
      }
    }
    case SuperStepComplete(aggregators) =>
      log.info(s"Left : ${successReceived.get()}")

      aggregators match {
        case Some(aggregatorMapping) =>
          if (_aggregatorMapping.isEmpty) {
            _aggregatorMapping ++= aggregatorMapping
          } else {
            aggregatorMapping.foreach{
              case (name, aggregator) =>
                _aggregatorMapping(name).aggregate(aggregator)
                println("MASTER aggregate " + name + " " + _aggregatorMapping(name).value)
            }
          }
        case None =>
          // noop
      }

      if (successReceived.decrementAndGet() == 0) {
        successReceived = new AtomicInteger(allWorkers().size)

        FutureUtil.callbackOnAllCompleteWithResults(allWorkers().map(x => (x.actorRef ? AllSuperStepComplete()).mapTo[DoNextStep])) {
          implicit results =>
            log.info("Do next step ?")
            val doNextStep = results.map(_.yes).reduce(_ || _)
            log.info(s"${results.size}")
            log.info(s"${results.map(_.toString).reduce(_ + " " + _)}")
            log.info(s"Hell $doNextStep")
            // set to superstep
            if (doNextStep) {
              // TODO elasticity
              doingElasticity = true
              doElasticity().foreach(newWorkersMapping => {
                workerIdAddressMap = newWorkersMapping
                currentSuperStep += 1
                doingElasticity = false
                allWorkers().foreach(_.actorRef ! DoBarrier(currentSuperStep, _aggregatorMapping.toMap))
              })

            } else {
              sendDoOutput()
              log.info(s"we're done at step ${currentSuperStep}")
            }
        }
      }
    case DoneOutput() =>
      log.info(s"Output left: ${doneDoOutput.get}")
      if (doneDoOutput.decrementAndGet() == 0) {
        // print dem aggregators
        _aggregatorMapping.foreach{case (name, aggregator) => println(s"$name: ${aggregator.value}")}
        log.info("All output done, send shutdown.")
        terminate()
      }
  }


  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Terminating master")
    context.system.terminate()
  }

  def terminate(): Unit = {
    log.info("Initiating cluster termination")
    allWorkers().foreach(x => context.stop(x.actorRef))
    context.stop(self)
  }
}

// TODO rework this, kind of ugly :p
class ElasticityPromise(currentWorkers : Map[Int, AddressPair], implicit val askTimeout : Timeout, implicit val executionContext : ExecutionContext) extends Promise[Map[Int, AddressPair]] {

  private[this] val defaultPromise : Promise[Map[Int, AddressPair]] = Promise[Map[Int, AddressPair]]
  private[this] var _numWorkersExpected : Option[Int] = None
  private[this] var nextWorkers : TrieMap[Int, AddressPair] = TrieMap.empty
  private[this] var newWorkers : TrieMap[Int, AddressPair] = TrieMap.empty
  private[this] val elasticGrowComplete = new AtomicInteger(0)
  private[this] val workerCounter : AtomicInteger = new AtomicInteger(0)
  nextWorkers ++= currentWorkers

  def numWorkersExpected(numWorkers : Int) = {
    _numWorkersExpected = Some(numWorkers)
    sendElasticGrowIfCompleted()
  }

  def newWorker(index : Int, addressPair: AddressPair) = synchronized {
    workerCounter.incrementAndGet()
    nextWorkers += index -> addressPair
    newWorkers += index -> addressPair
    sendElasticGrowIfCompleted()
  }

  def elasticGrowCompleted(): Unit = {
    if (elasticGrowComplete.incrementAndGet() == currentWorkers.size) {

      val nextWorkersMap = nextWorkers.toMap
      // set state to superstep
      FutureUtil.callbackOnAllComplete(nextWorkersMap.map(_._2.actorRef).map(_ ? State(GlobalState.SUPERSTEP))) {
        this.success(nextWorkersMap)
      }
    }
  }

  private[this] def sendElasticGrowIfCompleted(): Unit = synchronized {
    _numWorkersExpected match {
      case Some(x) =>
        if (x == workerCounter.get()) { // got all workers, now we can recreate
          // distribute
          // TODO distribute
          val nextWorkersMap = nextWorkers.toMap
          val currentWorkersMap = currentWorkers.toMap
          val newWorkersMap = newWorkers.toMap

          // distribute workers mapping
          FutureUtil.callbackOnAllComplete(currentWorkers.map(_._2.actorRef).map(_ ? NewWorkerMap(newWorkersMap))) {
            FutureUtil.callbackOnAllComplete(newWorkers.map(_._2.actorRef).map(_ ? NewWorkerMap(nextWorkersMap))) {
              //set state for all
              FutureUtil.callbackOnAllComplete(nextWorkers.map(_._2.actorRef).map(_ ? State(GlobalState.GROW_ELASTIC))) {
                //only currentworkers distribute
                currentWorkers.map(_._2.actorRef).foreach(_ ! ElasticGrow(currentWorkersMap, nextWorkersMap))
              }
            }
          }


        }
        // noop
      case None =>
        // noop
    }
  }



  override def future: Future[Map[Int, AddressPair]] = defaultPromise.future

  override def tryComplete(result: Try[Map[Int, AddressPair]]): Boolean = defaultPromise.tryComplete(result)

  override def isCompleted: Boolean = defaultPromise.isCompleted
}
