package io.joygraph.impl.hadoop.reader

import java.io.IOException

import com.typesafe.config.Config
import io.joygraph.core.reader.LineProvider
import io.joygraph.core.util.IOUtil
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
    var lineReaderOpt : Option[mapreduce.RecordReader[LongWritable, Text]] = None
    var lineReadersOpt : Option[Array[mapreduce.RecordReader[LongWritable, Text]]] = None
    try {
      val dfsPath = new Path(path)
      // TODO MAPREDUCE-6635 bug in 2.7.2, where splits cannot be larger than Integer.Max, switch to 2.7.3 when released
      // split length into chunks of maximum Integer.MAX
      val iterator : Iterator[String] =
        if (length > Integer.MAX_VALUE) {
          val numSplits = length / Integer.MAX_VALUE + 1
          if (numSplits > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("The number of splits is larger than Integer.MAX_VALUE");
          }
          val lineReaders = IOUtil.splits(length, numSplits.toInt, start).map{
            case (position, splitLength) =>
              val lineReader : mapreduce.RecordReader[LongWritable, Text] = textInputFormat.createRecordReader(null, taskAttemptContext)
              lineReader.initialize(new FileSplit(dfsPath, position, splitLength, null), taskAttemptContext)
              lineReader
          }
          lineReadersOpt = Some(lineReaders)
          new Iterator[String] {
            var currentLineReaderIndex : Int = 0
            override def hasNext: Boolean = if(lineReaders(currentLineReaderIndex).nextKeyValue()) {
              true
            } else {
              if (currentLineReaderIndex == lineReaders.length - 1) {
                false
              } else {
                currentLineReaderIndex += 1
                lineReaders(currentLineReaderIndex).nextKeyValue()
              }
            }

            override def next(): String = lineReaders(currentLineReaderIndex).getCurrentValue.toString
          }

        } else {
          val lineReader : mapreduce.RecordReader[LongWritable, Text] = textInputFormat.createRecordReader(null, taskAttemptContext)
          lineReader.initialize(new FileSplit(dfsPath, start, length, null), taskAttemptContext)
          lineReaderOpt = Some(lineReader)
          new Iterator[String] {
            override def hasNext: Boolean = lineReader.nextKeyValue()
            override def next(): String = lineReader.getCurrentValue.toString
          }
        }

      // Does not implement hasNext properly, as nextKeyValue READS the next value
      // and getCurrentValue does not advance the pointer.
      // so it works for a one-time traversal, but not when hasNext or next is explicitly called
      f(iterator)
    } catch {
      case e : IOException => e.printStackTrace()
      case e : Throwable => e.printStackTrace()
    } finally {
      lineReaderOpt match {
        case Some(x) =>
          x.close()
        case None =>
          // noop
      }
      lineReadersOpt match {
        case Some(lineReaders) =>
          lineReaders.foreach(_.close())
        case None =>
          // noop
      }

    }
  }

}
