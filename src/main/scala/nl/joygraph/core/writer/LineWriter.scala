package nl.joygraph.core.writer

import java.io.OutputStream

import com.typesafe.config.Config

trait LineWriter {
  def write(conf: Config, path : String, streamWriter: (OutputStream) => Any)
}
