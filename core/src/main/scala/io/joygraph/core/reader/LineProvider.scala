package io.joygraph.core.reader

import com.typesafe.config.Config

trait LineProvider {
  def read(conf: Config, path : String, start : Long, length : Long)(f : (Iterator[String]) => Any): Unit

}
