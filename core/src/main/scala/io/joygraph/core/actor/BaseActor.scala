package io.joygraph.core.actor

import java.util

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.typesafe.config._
import io.joygraph.core.config.JobSettings

class BaseActor(private[this] val jobConf : Config, masterFactory : (Config, Cluster) => _ <: Master, workerFactory : () => Worker[_,_,_]) extends Actor with ActorLogging {

  private[this] val cluster = Cluster(context.system)
  private[this] var workerRef : Option[ActorRef] = None
  private[this] var masterRef : Option[ActorRef] = None
  // TODO assume seedNodeAddresses == 1
  private[this] val seedNodeAddresses : util.List[String] = jobConf.getStringList("akka.cluster.seed-nodes")

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    //#subscribe
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember], classOf[LeaderChanged])
    //#subscribe
    assert(seedNodeAddresses.size() == 1)
    if (seedNodeAddresses.contains(cluster.selfAddress.toString)) {
      spawnMaster()
    } else {
      spawnWorker()
    }
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case m @ MemberUp(member) =>
      masterRef match {
        case Some(_) =>
          log.info("Member is Up: {}", member.address)
          masterRef.foreach(_ ! m)
        case None =>
      }
    case m @ UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
//      workerRef match {
//        case Some(x) =>
//          x ! m
//        case None =>
//      }
    case LeaderChanged(leader) =>
      log.info("Leader Changed {}", leader)
    case x : MemberEvent => // ignore
      println("unhandled member event " + x)
    case x @ _ =>
      println("what's this " + x)
  }

  def suicide(): Unit = {
    self ! PoisonPill
  }

  def masterProps : Props = {
    Props(masterFactory(jobConf, cluster))
  }
  def workerProps : Props = {
    Props(workerFactory())
  }

  def spawnWorker(): Unit = {
    workerRef = Option(context.system.actorOf(workerProps, JobSettings(jobConf).workerSuffix))
  }

  def spawnMaster() : Unit = {
    masterRef = Option(context.system.actorOf(masterProps, JobSettings(jobConf).masterSuffix))
  }

}
