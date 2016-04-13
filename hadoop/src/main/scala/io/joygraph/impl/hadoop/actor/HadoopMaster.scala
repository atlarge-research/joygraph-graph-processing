package io.joygraph.impl.hadoop.actor

import com.typesafe.config.Config
import io.joygraph.core.actor.Master
import io.joygraph.core.util.IOUtil
import io.joygraph.impl.hadoop.util.FileSystemMap
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hdfs.HdfsConfiguration

trait HadoopMaster extends Master with FileSystemMap {
  protected[this] val conf : Config
  private[this] val hdfsConfiguration = new HdfsConfiguration() // load from classpath

  override protected[this] def mkdirs(path : String) : Boolean = {
    implicit val conf = hdfsConfiguration
    val hadoopPath = new Path(path)
    fs(hadoopPath).mkdirs(hadoopPath)
  }

  override protected[this] def split(workerId: Int, totalNumNodes : Int, path : String): (Long, Long) = {
    implicit val conf = hdfsConfiguration
    val hadoopPath = new Path(path)
    var fileStatus : FileStatus = null
    try {
      fileStatus = fs(hadoopPath).getFileStatus(hadoopPath)
    } catch {
      case (x : Throwable) =>
        // TODO throw exception
        println(x + " fagaga")
    }

    val size: Long = fileStatus.getLen
    IOUtil.split(workerId, size, totalNumNodes)
  }
}
