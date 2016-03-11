package io.joygraph.core.config

import com.typesafe.config.Config
import io.joygraph.core.reader.LineProvider
import io.joygraph.core.writer.LineWriter

case class JobSettings(private val conf : Config) {
  val initialNumberOfWorkers : Int = conf.getInt("job.workers.initial")
  val dataPath : String = conf.getString("job.data.path")
  val outputPath : String = conf.getString("job.output.path")
  val inputDataLineProvider : Class[_ <: LineProvider]= Class.forName(conf.getString("worker.input.lineProviderClass")).asInstanceOf[Class[LineProvider]]
  val outputDataLineWriter : Class[_ <: LineWriter] = Class.forName(conf.getString("worker.output.lineWriterClass")).asInstanceOf[Class[LineWriter]]
  val masterSuffix : String = conf.getString("master.suffix")
  val workerSuffix : String = conf.getString("worker.suffix")
}