package io.joygraph.impl.hadoop.writer

import java.io.{IOException, OutputStream}

import com.typesafe.config.Config
import io.joygraph.core.writer.LineWriter
import io.joygraph.impl.hadoop.util.FileSystemMap
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.hdfs.HdfsConfiguration

class HadoopLineWriter extends LineWriter with FileSystemMap {

  override def write(conf : Config, path : String, streamWriter: (OutputStream) => Any): Unit = {
    implicit val hdfsConf = new HdfsConfiguration() // load from classpath
    hdfsConf.setInt("io.file.buffer.size", 65536)
    val dfsPath = new Path(path)

    var os : FSDataOutputStream = null
    var fileSystem : FileSystem = null
    try {
      fileSystem = fs(dfsPath)
      os = fileSystem.create(dfsPath, true)
      // consume
      streamWriter(os)
    } catch {
      case (e : IOException) =>
        // todo report exception
        e.printStackTrace()
      case (e : Throwable) =>
        // todo report exception
      e.printStackTrace()
    } finally {
      if (os != null) {
        os.hflush()
        os.close()
      }
      if (fileSystem != null) {
        fileSystem.close()
      }
    }
  }
}
