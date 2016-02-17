package nl.joygraph.impl.hadoop.actor

import akka.cluster.Cluster
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem}

class Master(conf : Config, cluster : Cluster) extends nl.joygraph.core.actor.Master(conf, cluster) {
  val hadoopConfiguration = new Configuration(false)
  hadoopConfiguration.set("fs.defaultFS", conf.getString("fs.defaultFS"))
  private[this] val fs = FileSystem.get(hadoopConfiguration)

  override protected[this] def split(workerId: Int, totalNumNodes : Int, path : String): (Long, Long) = {
    var fileStatus : FileStatus = null
    try {
      fileStatus = fs.getFileStatus(new org.apache.hadoop.fs.Path(path))
    } catch {
      case (x : Throwable) =>
        println(x + " fagaga")
    }
    val size: Long = fileStatus.getLen
    var position: Long = 0L

    if (workerId == 0) {
      position = 0
    }
    else {
      position = (size / totalNumNodes) * workerId
    }
    val len: Long = size / totalNumNodes

    (position, len)
  }
}
