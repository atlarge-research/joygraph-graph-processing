package io.joygraph.core.actor

import java.util

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.typesafe.config._
import io.joygraph.core.config.JobSettings

class BaseActor(private[this] val jobConf : Config, masterFactory : (Config, Cluster) => _ <: Master, workerFactory : () => Worker[_,_,_]) extends Actor with ActorLogging {

  private[this] val cluster = Cluster(context.system)
  private[this] var leader : Option[Address] = None
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
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      masterRef.foreach(_ ! MemberUp(member))
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case LeaderChanged(address) =>
      println("Leader Changed {}", address)
//      changeToClass(address);
    case x : MemberEvent => // ignore
      println("agaga " + x)
    case _ =>
      println("what the fuck")
  }

  @deprecated
  def changeToClass(address : Option[Address]): Unit = {
    synchronized {
      leader match {
        case Some(x) =>
          // leader changed due to unreachability
          suicide()
        case None =>
          leader = address
          leader.foreach( x =>
            if(x == cluster.selfAddress) {
              spawnMaster()
            } else {
              spawnWorker()
            }
          )
      }
    }
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
