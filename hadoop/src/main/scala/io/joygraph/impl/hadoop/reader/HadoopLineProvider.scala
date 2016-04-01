package io.joygraph.impl.hadoop.reader

import java.io.IOException

import com.typesafe.config.Config
import io.joygraph.core.reader.LineProvider
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

class HadoopLineProvider extends LineProvider {

  def read(conf: Config, path : String, start : Long, length : Long)(f : (Iterator[String]) => Any): Unit = {
    val textInputFormat = new TextInputFormat()
    val taskAttemptId = new TaskAttemptID()
    // load from classpath
    val hdfsConf = new HdfsConfiguration()
    hdfsConf.setInt("io.file.buffer.size", 65536)
    val taskAttemptContext = new TaskAttemptContextImpl(hdfsConf, taskAttemptId)
    val lineReader : mapreduce.RecordReader[LongWritable, Text] = textInputFormat.createRecordReader(null, taskAttemptContext)
    try {
      val dfsPath = new Path(path)
      // TODO bug in 2.7.2, where splits cannot be larger than Integer.Max, switch to 2.7.3 when released
      lineReader.initialize(new FileSplit(dfsPath, start, length, null), taskAttemptContext)
      // Does not implement hasNext properly, as nextKeyValue READS the next value
      // and getCurrentValue does not advance the pointer.
      // so it works for a one-time traversal, but not when hasNext or next is explicitly called
      val iterator : Iterator[String] = new Iterator[String] {
        override def hasNext: Boolean = lineReader.nextKeyValue()
        override def next(): String = lineReader.getCurrentValue.toString
      }
      f(iterator)
    } catch {
      case e : IOException => e.printStackTrace()
      case e : Throwable => e.printStackTrace()
    } finally {
      lineReader.close()
    }
  }

}
