package nl.joygraph.impl.hadoop.reader

import com.typesafe.config.Config
import nl.joygraph.core.reader.LineProvider
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

class HadoopLineProvider extends LineProvider {

  override protected[this] var _path: String = _
  override protected[this] var _length: Long = _
  override protected[this] var _start: Long = _
  private[this] var lineReader : mapreduce.RecordReader[LongWritable, Text] = _

  override def initialize(conf : Config) = {
    val textInputFormat = new TextInputFormat()
    val taskAttemptId = new TaskAttemptID()
    val newConf = new Configuration(false)
    newConf.set("fs.defaultFS", conf.getString("fs.defaultFS"))
    newConf.setInt("io.file.buffer.size", 65536)
    val taskAttemptContext = new TaskAttemptContextImpl(newConf, taskAttemptId)
    lineReader = textInputFormat.createRecordReader(null, taskAttemptContext)
    val dfsPath = new Path(path)
    lineReader.initialize(new FileSplit(dfsPath, start, length, null), taskAttemptContext)
  }

  /**
    * Does not implement hasNext properly, as nextKeyValue READS the next value
    * and getCurrentValue does not advance the pointer.
    * @return
    */
  override def iterator: Iterator[String] = new Iterator[String] {
    override def hasNext: Boolean = lineReader.nextKeyValue()
    override def next(): String = lineReader.getCurrentValue.toString
  }
}
