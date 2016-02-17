package nl.joygraph.core.reader

import com.typesafe.config.Config

trait LineProvider extends Iterable[String] {
  protected[this] var _path : String
  protected[this] var _start : Long
  protected[this] var _length : Long

  def path_=(path: String) = _path = path
  def path : String = _path
  def start_=(start: Long) = _start = start
  def start : Long = _start
  def length_=(length: Long) = _length = length
  def length : Long = _length

  def initialize(conf : Config)
}
