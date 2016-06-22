package io.joygraph.core.actor

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor.{Actor, ActorLogging, ActorRef, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.metrics.{ClusterMetricsChanged, ClusterMetricsExtension}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Shrink}
import io.joygraph.core.actor.elasticity.{ElasticityHandler, ElasticityPromise, WorkerProviderProxy}
import io.joygraph.core.actor.metrics.{WorkerOperation, WorkerStateRecorder}
import io.joygraph.core.actor.state.GlobalState
import io.joygraph.core.config.JobSettings
import io.joygraph.core.message._
import io.joygraph.core.message.elasticity._
import io.joygraph.core.message.superstep._
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import io.joygraph.core.program.Aggregator
import io.joygraph.core.util.{Errors, ExecutionContextUtil, FutureUtil}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

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
  var akkaAddressToWorkerIdMap : Map[Address, Int] = Map.empty
  var doneDoOutput : AtomicInteger = _
  val initializationLock = new Semaphore(1)
  // TODO keep state in Master explicitly instead of implicitly
  var doingElasticity : Boolean = false
  val elasticityHandler : ElasticityHandler = new ElasticityHandler(this, cluster, timeout, executionContext)

  // An actor providing actors
  protected[this] var workerProviderProxyOption : Option[WorkerProviderProxy] = None

  private[this] var initialized: Boolean = false
  private[this] var _aggregatorMapping : scala.collection.mutable.Map[String, Aggregator[_]] = mutable.OpenHashMap.empty
  val numVertices = new AtomicLong(0)
  val numEdges = new AtomicLong(0)

  private[this] var currentSuperStep = 0

  private[this] val allLoadDataCompleteReceived = new AtomicInteger
  private[this] val barrierSuccessReceived = new AtomicInteger
  private[this] val loadDataSuccessReceived = new AtomicInteger()
  private[this] val superStepSuccessReceived = new AtomicInteger()
  private[this] val policy : ElasticPolicy = jobSettings.policy
  private[this] val workerStateRecorder : WorkerStateRecorder = new WorkerStateRecorder
  policy.init(jobSettings.policySettings, workerStateRecorder)

  private[this] var currentPartitioner : VertexPartitioner = _

  def workerMembers() = cluster.state.members.filterNot(_.address == cluster.selfAddress)

  def setWorkerProvider(workerProvider : ActorRef) = this.workerProviderProxyOption = Option(new WorkerProviderProxy(workerProvider))

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
      val assignedFuture: Future[Unit] = fActorRef.map {
        case AddressPair(actorRef, NettyAddress(host, port)) =>
          // TODO abstract hostname transformation
          workerIdAddressMapBuilder(currentWorkerId) = AddressPair(actorRef, NettyAddress( if(host.startsWith("node")) host+".ib.cluster" else host, port))
      }
      workerId += 1
      assignedFuture
    }) {
      workerIdAddressMap = workerIdAddressMapBuilder.toMap
      akkaAddressToWorkerIdMap = workerIdAddressMap.mapValues(_.actorRef.path.address).map(_.swap)
      log.info("Distributing ActorRefs")
      val actorSelections: Iterable[ActorRef] = allWorkers().map(_.actorRef)

      FutureUtil.callbackOnAllComplete(sendMasterAddress(actorSelections)) {
        FutureUtil.callbackOnAllComplete(sendMapping(actorSelections)) {
          sendPaths(workerIdAddressMap, jobSettings.dataPath)
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

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    ClusterMetricsExtension.get(context.system).subscribe(self)
  }

  def sendPaths(workerIdToAddress : Map[Int, AddressPair], dataPath : String) = {
    log.info(s"Sending paths to ${workerIdToAddress.size} workers")
    val workerActorRefs = workerIdToAddress.map(_._2.actorRef)
    // set the state
    FutureUtil.callbackOnAllComplete(workerActorRefs.map(x => (x ? State(GlobalState.LOAD_DATA)).mapTo[Boolean])) {
      log.info("State set to LOAD_DATA")
      log.info("Sending PrepareLoadData")
      FutureUtil.callbackOnAllComplete(workerActorRefs.map(x => (x ? PrepareLoadData()).mapTo[Boolean])) {
        loadDataSuccessReceived.set(workerIdToAddress.size)
        workerIdToAddress.foreach {
          case (index, AddressPair(actor, _)) =>
            val (position, length) = split(index, workerMembers().size, dataPath)
            logWorkerActionStart(actor.path.address, WorkerOperation.LOAD_DATA)
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
      workerIdAddressMap.keys.foreach {
        workerStateRecorder.workerStateStart(currentSuperStep, _, WorkerOperation.PREPARE_FIRST_SUPERSTEP, System.currentTimeMillis())
      }
      FutureUtil.callbackOnAllComplete(allWorkers().map(x => (x.actorRef ? PrepareSuperStep(numVertices, numEdges)).mapTo[Boolean])) {
        workerIdAddressMap.keys.foreach {
          workerStateRecorder.workerStateStop(currentSuperStep, _, WorkerOperation.PREPARE_FIRST_SUPERSTEP, System.currentTimeMillis())
        }
        log.info("Sending runsuperstep")
        superStepSuccessReceived.set(allWorkers().size)
        logMasterAction("RunSuperStep", currentSuperStep)
        allWorkers().foreach(worker => logWorkerActionStart(worker.actorRef.path.address, WorkerOperation.RUN_SUPERSTEP))
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
    val elasticityPromise = new ElasticityPromise

    workerProviderProxyOption match {
      case Some(workerProviderProxy) =>
        val policyResult = Option(currentPartitioner) match {
          case Some(x) => policy.decide(currentSuperStep, workerIdAddressMap, x)
          case None => policy.decide(currentSuperStep, workerIdAddressMap, new VertexHashPartitioner(allWorkers().size)) // TODO remove this when partitioner is propagated from master from the beginning
        }
        // elasticityPromise must be fulfilled by decide method
        policyResult match {
          case Some(result) =>
            currentPartitioner = result match {
              case Shrink(_, partitioner) => partitioner
              case Grow(_, partitioner) => partitioner
            }
            elasticityHandler.startElasticityOperation(ElasticityHandler.ElasticityOperation(result, elasticityPromise, workerIdAddressMap), workerProviderProxy, conf)
          case None =>
            log.info("No grow or shrink")
            elasticityPromise.success(workerIdAddressMap)
        }
      case None =>
        log.info("There's no provider for elasticity")
        elasticityPromise.success(workerIdAddressMap)
    }

    elasticityPromise.future
  }

  def logWorkerActionStart(workerAkkaAddress : Address, workerOp : WorkerOperation.Value): Unit = {
    akkaAddressToWorkerIdMap.get(workerAkkaAddress) match {
      case Some(workerId) =>
        workerStateRecorder.workerStateStart(currentSuperStep, workerId, workerOp, System.currentTimeMillis())
      case None =>
    }
  }

  def logWorkerActionStop(workerAkkaAddress : Address, workerOp : WorkerOperation.Value): Unit = {
    akkaAddressToWorkerIdMap.get(workerAkkaAddress) match {
      case Some(workerId) =>
        workerStateRecorder.workerStateStop(currentSuperStep, workerId, workerOp, System.currentTimeMillis())
      case None =>
    }
  }

  def logWorkerAction(workerAkkaAddress : Address, keyword : String, message : Any) = {
    akkaAddressToWorkerIdMap.get(workerAkkaAddress) match {
      case Some(workerId) =>
        log.info("{} : {} : {}", keyword, workerId, message)
      case None =>
        log.info("{} : {} : {}", keyword, "Unknown", message)
    }
  }

  def logMasterAction(keyword : String, message : Any): Unit = {
    log.info("{} : {} : {}", keyword, "M", message)
  }

  override def receive = {
    case ClusterMetricsChanged(nodeMetrics) =>
      policy.addMetrics(currentSuperStep, nodeMetrics, akkaAddressToWorkerIdMap)
      nodeMetrics.foreach {
        x =>
          if (cluster.selfAddress == x.address) {
            println("#M#" + x)
          } else {
            akkaAddressToWorkerIdMap.get(x.address) match {
              case Some(workerId) =>
                println("#" + workerId + "#" + x)
              case None =>
                println("#?#" + x)
            }
          }
      }
    case InitiateTermination() =>
      terminate()
    case RegisterWorkerProvider(workerProviderRef) =>
      setWorkerProvider(workerProviderRef)
    case ElasticityComplete() => {
      logWorkerActionStart(sender().path.address, WorkerOperation.DISTRIBUTE_DATA)
      elasticityHandler.elasticityCompleted()
    }
    case wResponse @ WorkersResponse(numWorkers) =>
      // these are the number of workers that will be launched
      // we assume that the requested workers will be received
      workerProviderProxyOption match {
        case Some(workerProviderProxy) =>
          log.info("Received response from workerProvider")
          workerProviderProxy.response(wResponse)
        case None =>
          log.error("Getting a response when there's no provider is impossible")
      }
    case NewWorker() =>
      val senderRef = sender()
      elasticityHandler.newWorker(senderRef)
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
      log.info(s"$workerId $numV $numEdge done")
      val senderRef = sender()
      logWorkerAction(senderRef.path.address, "LoadingComplete", "PROCESS_COMPLETE")
      logWorkerActionStop(senderRef.path.address, WorkerOperation.LOAD_DATA)
      numVertices.addAndGet(numV)
      numEdges.addAndGet(numEdge)
      if (allLoadDataCompleteReceived.decrementAndGet() == 0) {
        log.info(s"total : ${numVertices.get} ${numEdges.get}")
        // set to superstep
        sendSuperStepState(numVertices.get, numEdges.get)
      }
    case AllLoadingComplete() =>
      if (loadDataSuccessReceived.decrementAndGet() == 0) {
        logWorkerAction(sender().path.address, "AllLoadingComplete", "LOCAL")
        allLoadDataCompleteReceived.set(allWorkers().size)
        allWorkers().foreach(_.actorRef ! AllLoadingComplete())
      }
    case BarrierComplete() => {
      val senderRef = sender()
      logWorkerAction(senderRef.path.address, "Barrier", currentSuperStep)
      logWorkerActionStop(senderRef.path.address, WorkerOperation.BARRIER)

      if (barrierSuccessReceived.decrementAndGet() == 0) {
        logMasterAction("BarrierComplete", currentSuperStep)
        //reset aggregator
        _aggregatorMapping.foreach{
          case (name, aggregator) =>
            _aggregatorMapping(name).masterPrepareStep()
        }
        superStepSuccessReceived.set(allWorkers().size)
        logMasterAction("RunSuperStep", currentSuperStep)
        allWorkers().foreach { worker =>
          logWorkerActionStart(worker.actorRef.path.address, WorkerOperation.RUN_SUPERSTEP)
        }
        allWorkers().foreach(_.actorRef ! RunSuperStep(currentSuperStep))
      }
    }
    case SuperStepComplete(aggregators) =>
      log.info(s"Left : ${superStepSuccessReceived.get()}")
      val senderRef = sender()
      logWorkerAction(senderRef.path.address, "SuperStepComplete", currentSuperStep)
      logWorkerActionStop(senderRef.path.address, WorkerOperation.RUN_SUPERSTEP)

      aggregators match {
        case Some(aggregatorMapping) =>
          if (_aggregatorMapping.isEmpty) {
            _aggregatorMapping ++= aggregatorMapping
          } else {
            aggregatorMapping.foreach{
              case (name, aggregator) =>
                _aggregatorMapping(name).aggregate(aggregator)
                log.info("MASTER aggregate " + name + " " + _aggregatorMapping(name).value)
            }
          }
        case None =>
          // noop
      }

      if (superStepSuccessReceived.decrementAndGet() == 0) {
        logMasterAction("SuperStepComplete", currentSuperStep)
        FutureUtil.callbackOnAllCompleteWithResults(allWorkers().map(x => (x.actorRef ? AllSuperStepComplete()).mapTo[DoNextStep])) {
          implicit results =>
            log.info("Do next step ?")
            val doNextStep = results.map(_.yes).reduce(_ || _)
            log.info(s"${results.size}")
            log.info(s"${results.map(_.toString).reduce(_ + " " + _)}")
            log.info(s"Hell $doNextStep")
            // set to superstep
            if (doNextStep) {
              doingElasticity = true
              doElasticity().foreach( newWorkersMapping => {
                workerIdAddressMap = newWorkersMapping
                akkaAddressToWorkerIdMap = workerIdAddressMap.mapValues(_.actorRef.path.address).map(_.swap)
                currentSuperStep += 1
                doingElasticity = false
                barrierSuccessReceived.set(allWorkers().size)
                logMasterAction("DoBarrier", currentSuperStep)
                allWorkers().foreach(worker => logWorkerActionStart(worker.actorRef.path.address, WorkerOperation.BARRIER))
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
      logWorkerAction(sender().path.address, "DoneOutput", "LOCAL")
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
    allWorkers().foreach(x => cluster.leave(x.actorRef.path.address))
    allWorkers().foreach(x => context.stop(x.actorRef))
    context.stop(self)
  }
}