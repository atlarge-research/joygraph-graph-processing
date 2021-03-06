package io.joygraph.core.actor.elasticity

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.cluster.Cluster
import akka.util.Timeout
import com.typesafe.config.Config
import io.joygraph.core.actor.Master
import io.joygraph.core.actor.elasticity.ElasticityHandler.ElasticityOperation
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Shrink}
import io.joygraph.core.actor.metrics.{SupplyDemandMetrics, WorkerOperation}
import io.joygraph.core.actor.state.GlobalState
import io.joygraph.core.message.elasticity.{NewWorkerMap, _}
import io.joygraph.core.message.{AddressPair, State, WorkerId}
import io.joygraph.core.util.{FutureUtil, IOUtil}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

object ElasticityHandler {
  final case class ElasticityOperation(result : ElasticPolicy.Result, promise : ElasticityPromise, currentWorkers : Map[Int, AddressPair])
}

class ElasticityHandler(master : Master, cluster : Cluster, policy : ElasticPolicy,
                        implicit val askTimeout : Timeout,
                        implicit val executionContext : ExecutionContext) {
  private[this] var _currentOperation : ElasticityOperation = _
  private[this] val _newWorkerIds : LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()

  private[this] var nextWorkers : TrieMap[Int, AddressPair] = TrieMap.empty
  private[this] var newWorkers : TrieMap[Int, AddressPair] = TrieMap.empty
  private[this] val elasticityComplete = new AtomicInteger(0)
  private[this] val workerCounter : AtomicInteger = new AtomicInteger(0)

  private[this] var _currentSuperStep : Int = _

  def startElasticityOperation(currentSuperStep : Int, operation: ElasticityOperation, workerProviderProxy: WorkerProviderProxy, jobConf : Config) = {
    _currentOperation = operation
    _currentSuperStep = currentSuperStep

    nextWorkers.clear()
    newWorkers.clear()
    elasticityComplete.set(0)
    workerCounter.set(0)

    val ElasticityOperation(result, _, currentWorkers) = operation
    nextWorkers ++= currentWorkers

    result match {
      case Shrink(workersToRemove, partitioner) =>
        workersToRemove.foreach(nextWorkers.remove)
        policy.addSupplyDemand(SupplyDemandMetrics(_currentSuperStep, System.currentTimeMillis(), currentWorkers.size, currentWorkers.size - workersToRemove.size))
        sendElasticityOperationIfCompleted()
      case Grow(workersToAdd, partitioner) =>
        // clear and set new ids
        _newWorkerIds.clear()
        workersToAdd.foreach(_newWorkerIds.add)
        policy.addSupplyDemand(SupplyDemandMetrics(_currentSuperStep, System.currentTimeMillis(), currentWorkers.size, currentWorkers.size + workersToAdd.size))
        workerProviderProxy.requestWorkers(jobConf, workersToAdd.size).foreach { r =>
          sendElasticityOperationIfCompleted()
        }
    }
  }

  // for growing
  def newWorker(newWorkerActorRef : ActorRef) = synchronized {
    import akka.pattern.ask
    val nextWorkerId = _newWorkerIds.poll()
    val fAddressPair = (newWorkerActorRef ? WorkerId(nextWorkerId)).mapTo[AddressPair]

    fAddressPair.foreach {
      case AddressPair(actorRef, nettyAddress) =>
        // TODO abstract hostname transformation
        workerCounter.incrementAndGet()
        nextWorkers += nextWorkerId -> AddressPair(actorRef, IOUtil.infiniband(nettyAddress))
        newWorkers += nextWorkerId -> AddressPair(actorRef, IOUtil.infiniband(nettyAddress))
        sendElasticityOperationIfCompleted()
    }
  }

  // callback when distribution is complete for a worker
  def elasticityCompleted(): Unit = {
    import akka.pattern.ask
    val currentWorkers = _currentOperation.currentWorkers
    if (elasticityComplete.incrementAndGet() == currentWorkers.size) {
      val nextWorkersMap = nextWorkers.toMap

      FutureUtil.callbackOnAllComplete((currentWorkers ++ nextWorkersMap).map(_._2.actorRef).map(_ ? ElasticCleanUp())) {
        _currentOperation.result match {
          case Shrink(workersToRemove, partitioner) =>
            val workersToBeRemoved = currentWorkers.filterNot(x => nextWorkersMap.contains(x._1))
            // disconnect old workers' connections in network stack in remaining workers.
            val fut = FutureUtil.callbackOnAllComplete(currentWorkers.map(_._2.actorRef).map(_ ? ElasticRemoval(workersToBeRemoved))) {
              // remove old workers gracefully
              workersToBeRemoved.foreach(x => cluster.leave(x._2.actorRef.path.address))
              workersToBeRemoved.foreach(x => cluster.down(x._2.actorRef.path.address))
              // we wait for the worker to remove itself.
              workersToBeRemoved.foreach(x => cluster.system.stop(x._2.actorRef))
            }
            Await.ready(fut, Duration.Inf)
          case Grow(workersToAdd, partitioner) =>
          // noop
        }

        // set state to superstep
        FutureUtil.callbackOnAllComplete(nextWorkersMap.map(_._2.actorRef).map(_ ? State(GlobalState.SUPERSTEP))) {
          policy.addSupplyDemand(SupplyDemandMetrics(_currentSuperStep, System.currentTimeMillis(), nextWorkersMap.size, nextWorkersMap.size))
          _currentOperation.promise.success(nextWorkersMap)
        }
      }
    }
  }

  private[this] def sendElasticityOperationIfCompleted(): Unit = synchronized {
    import akka.pattern.ask

    val (run, partitioner) = _currentOperation.result match {
      case Shrink(workersToRemove, partitioner) =>
        (true, partitioner)
      case Grow(workersToAdd, partitioner) =>
        (workersToAdd.size == workerCounter.get(), partitioner)
    }

    val currentWorkers = _currentOperation.currentWorkers

    if (run) {
      // distribute
      val nextWorkersMap = nextWorkers.toMap
      val currentWorkersMap = currentWorkers
      val newWorkersMap = newWorkers.toMap
      val currentAndNewWorkersMap = currentWorkers ++ nextWorkersMap

      val globalState = State(GlobalState.ELASTIC_DISTRIBUTE)
      val elasticityMessage = ElasticDistribute(currentWorkersMap, nextWorkersMap, partitioner)

      // distribute workers mapping
      FutureUtil.callbackOnAllComplete(currentWorkers.map(_._2.actorRef).map(_ ? NewWorkerMap(newWorkersMap))) {
        FutureUtil.callbackOnAllComplete(newWorkers.map(_._2.actorRef).map(_ ? NewWorkerMap(currentAndNewWorkersMap))) {
          //set state for all
          FutureUtil.callbackOnAllComplete(currentAndNewWorkersMap.map(_._2.actorRef).map(_ ? globalState)) {
            // send partitioner for new workers
            FutureUtil.callbackOnAllComplete(newWorkers.map(_._2.actorRef).map(_ ? NewPartitioner(partitioner))) {

              //only currentworkers distribute
              currentWorkers.foreach{x => val worker = x._2.actorRef
                master.logWorkerActionStart(worker.path.address, WorkerOperation.DISTRIBUTE_DATA)
              }
              currentWorkers.map(_._2.actorRef).foreach(_ ! elasticityMessage)
            }
          }
        }
      }
    }
  }
}
