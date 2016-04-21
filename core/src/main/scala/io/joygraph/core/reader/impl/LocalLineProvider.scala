package io.joygraph.core.reader.impl

import java.io._

import com.typesafe.config.Config
import io.joygraph.core.reader.LineProvider
// TODO WIP for local tests
class LocalLineProvider extends LineProvider {
  override def read(conf: Config, path: String, start: Long, length: Long)(f: (Iterator[String]) => Any): Unit = {
    // assume the path is in the local file system
    val fis = new FileInputStream(path)
    fis.markSupported()
    fis.skip(start)
    val bis = new BoundedInputStream(fis, length)

    val stringIt : Iterator[String] = if (start == 0) {
      val iR = new InputStreamReader(bis)
      val bR = new BufferedReader(iR)

      new Iterator[String] {
        private[this] var _nextValue : String = null
        private[this] var _lastBufferPosition : Long = _
        private[this] var _isPartialLine : Boolean = false
        private[this] def nextValue() : Boolean = {
          val position = bis.position()
          _nextValue = bR.readLine()
          if (bis.available == 0) { // the Bounded stream has been consumed
            // TODO don't overwrite lastbufferposition on subsequent calls
            _lastBufferPosition = position
            // check whether it is the null
            if (_nextValue == null) {
              // check whether it is a partial string
              val raf = new RandomAccessFile(path, "r")
              // find the last linebreak
              raf.seek(start + _lastBufferPosition)
              var lastCharIsLineSeparator : Boolean = false
              while (raf.getChannel.position() < start + length) {
                lastCharIsLineSeparator = Character.LINE_SEPARATOR == Character.getType(raf.readChar())
              }
              if (lastCharIsLineSeparator) {
                _isPartialLine = false
              } else {
                // partial line needs to be completed
                _isPartialLine = true
              }
            }
          }
          _nextValue != null
        }

        override def hasNext: Boolean = nextValue()

        override def next(): String = _nextValue
      }
    } else { // start > 0 or start < 0, the latter is illegal so ..
      // always skip the first line
      fis.skip(start)

      new Iterator[String] {
        override def hasNext: Boolean = ???

        override def next(): String = ???
      }
    }
    f(stringIt)
  }
}

private class BoundedInputStream(is : InputStream, max : Long) extends InputStream {

  if (max < 0) throw new IllegalArgumentException("max >= 0")

  private[this] var _position : Long = 0

  override def read(): Int = {
    if (_position < max){
      _position += 1
      is.read()
    } else {
      -1
    }
  }

  def position(): Long = {
    _position
  }

  override def read(b: Array[Byte], off: Int, len: Int) : Int = {
    if (_position >= max) {
      return -1
    }
    val maxRead: Long = Math.min(len, max - _position)
    val bytesRead: Int = is.read(b, off, maxRead.toInt)
    if (bytesRead == -1) {
      -1
    } else {
      _position += bytesRead
      bytesRead
    }
  }

  override def skip(n : Long) = {
    val skippable = math.min(n, max - _position)
    _position += skippable
    is.skip(skippable)
  }

  override def available : Int = {
    if (_position >= max) {
      return 0
    }
    is.available
  }
}
