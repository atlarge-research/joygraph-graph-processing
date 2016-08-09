package io.joygraph.core.config

import com.typesafe.config.{Config, ConfigFactory}
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.reader.LineProvider
import io.joygraph.core.writer.LineWriter

case class JobSettings(private val conf : Config) {
  val policy : ElasticPolicy = if (conf.hasPath("job.policy.class")) Class.forName(conf.getString("job.policy.class")).newInstance().asInstanceOf[ElasticPolicy] else ElasticPolicy.default()
  val policySettings : Config = if (conf.hasPath("job.policy.settings")) conf.getConfig("job.policy.settings") else ConfigFactory.empty()
  val metricsPersistenceFilePath : Option[String] = if (conf.hasPath("job.metrics.persistence.file.path")) Some(conf.getString("job.metrics.persistence.file.path")) else None
  val programDefinition : String = conf.getString("job.program.definition.class")
  val masterMemory : Int = conf.getInt("job.master.memory")
  val masterCores : Int = conf.getInt("job.master.cores")
  val workerMemory : Int = conf.getInt("job.worker.memory")
  val workerCores : Int = conf.getInt("job.worker.cores")
  val nettyWorkers : Int = workerCores  // TODO own option
  val maxFrameLength : Int = if (conf.hasPath("network.maxframelength")) conf.getInt("network.maxframelength") else 1024 * 1024
  val maxEdgeSize : Int = if (conf.hasPath("maxedgesize")) conf.getInt("maxedgesize") else 4096 // must be <= maxFrameLength
  val initialNumberOfWorkers : Int = conf.getInt("job.workers.initial")
  val maxWorkers : Int = if (conf.hasPath("job.workers.max")) conf.getInt("job.workers.max") else initialNumberOfWorkers
  val dataPath : String = conf.getString("job.data.path")
  val verticesPath : Option[String] = if (conf.hasPath("job.vertices.path")) Some(conf.getString("job.vertices.path")) else None
  val outputPath : String = conf.getString("job.output.path")
  val inputDataLineProvider : Class[_ <: LineProvider]= Class.forName(conf.getString("worker.input.lineProviderClass")).asInstanceOf[Class[LineProvider]]
  val outputDataLineWriter : Class[_ <: LineWriter] = Class.forName(conf.getString("worker.output.lineWriterClass")).asInstanceOf[Class[LineWriter]]
  val masterSuffix : String = conf.getString("master.suffix")
  val workerSuffix : String = conf.getString("worker.suffix")
  val isDirected : Boolean = conf.getBoolean("job.directed")
}
