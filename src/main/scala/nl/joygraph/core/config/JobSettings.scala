package nl.joygraph.core.config

import com.typesafe.config.Config
import nl.joygraph.core.reader.LineProvider

case class JobSettings(private val conf : Config) {
  val initialNumberOfWorkers : Int = conf.getInt("job.workers.initial")
  val dataPath : String = conf.getString("job.data.path")
  val inputDataLineProvider : Class[_ <: LineProvider]= Class.forName(conf.getString("worker.input.lineProviderClass")).asInstanceOf[Class[LineProvider]]
  val masterSuffix : String = conf.getString("master.suffix")
  val workerSuffix : String = conf.getString("worker.suffix")
}
