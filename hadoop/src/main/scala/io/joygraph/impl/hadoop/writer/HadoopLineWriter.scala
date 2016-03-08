package io.joygraph.impl.hadoop.writer

import java.io.{IOException, OutputStream}

import com.typesafe.config.Config
import io.joygraph.core.writer.LineWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HadoopLineWriter extends LineWriter {

  override def write(conf : Config, path : String, streamWriter: (OutputStream) => Any): Unit = {
    val newConf = new Configuration(false)
    newConf.set("fs.defaultFS", conf.getString("fs.defaultFS"))
    newConf.setInt("io.file.buffer.size", 65536)
    val dfsPath = new Path(path)

    val fs = FileSystem.get(newConf)
    val os = fs.create(dfsPath, true)
    try {
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
      if (fs != null) {
        fs.close()
      }
    }
  }
}
