package io.joygraph.impl.hadoop.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.HdfsConfiguration

trait FileSystemMap {
  /**
    * @param path path to retrieve filesystem from
    * @param hdfsConfiguration the hdfs configuration from the classpath
    * @return
    */
  def fs(path : Path)(implicit hdfsConfiguration: HdfsConfiguration) : FileSystem = {
    path.getFileSystem(hdfsConfiguration)
  }
}
