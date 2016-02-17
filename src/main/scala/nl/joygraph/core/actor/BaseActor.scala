package nl.joygraph.core.actor

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.typesafe.config.Config
import nl.joygraph.core.config.JobSettings

class BaseActor(private[this] val jobConf : Config, masterFactory : (Cluster) => _ <: Master, workerFactory : () => Worker[_,_,_,_]) extends Actor with ActorLogging {

  private[this] val cluster = Cluster(context.system)
  private[this] var leader : Option[Address] = None
  private[this] var workerRef : Option[ActorRef] = None
  private[this] var masterRef : Option[ActorRef] = None

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    //#subscribe
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember], classOf[LeaderChanged])
    //#subscribe
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      masterRef.foreach(_ ! MemberUp(member))
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
      suicide()
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case LeaderChanged(address) =>
      println("Leader Changed {}", address)
      changeToClass(address);
    case x : MemberEvent => // ignore
      println("agaga " + x)
    case _ =>
      println("what the fuck")
  }

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
              spawnWorkerMaster(x)
            } else {
              spawnWorker(x)
            }
          )
      }
    }
  }

  def suicide(): Unit = {
    self ! PoisonPill
  }

  def masterProps : Props = {
    Props(masterFactory(cluster))
  }
  def workerProps : Props = {
    Props(workerFactory())
  }

  def spawnWorker(leader : Address): Unit = {
    workerRef = Option(context.system.actorOf(workerProps, JobSettings(jobConf).workerSuffix))
  }

  def spawnWorkerMaster(leader : Address) : Unit = {
    masterRef = Option(context.system.actorOf(masterProps, JobSettings(jobConf).masterSuffix))
    workerRef = Option(context.system.actorOf(workerProps, JobSettings(jobConf).workerSuffix))
  }

}
