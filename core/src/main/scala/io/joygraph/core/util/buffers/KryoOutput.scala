package io.joygraph.core.util.buffers

import java.io.OutputStream

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.ByteBufferOutput
import io.joygraph.core.util.DirectByteBufferGrowingOutputStream

object KryoOutput {
  class KryoOutputOverflowException extends KryoException
  val OVERFLOW_EXCEPTION = new KryoOutputOverflowException()
}

class KryoOutput(bufferSize : Int, maxBufferSize : Int) extends ByteBufferOutput(bufferSize, maxBufferSize) {

  override def require(required: Int): Boolean = try {
    super.require(required)
  } catch {
    case e : KryoException =>
      throw KryoOutput.OVERFLOW_EXCEPTION
  }

  override def setOutputStream(os : OutputStream): Unit = {
    super.setOutputStream(os)
    // TODO new PR
    // NOTE related https://github.com/EsotericSoftware/kryo/pull/394
    niobuffer.position(0)
  }

  override def flush(): Unit = {
    outputStream match {
      case os : DirectByteBufferGrowingOutputStream =>
        val buffer = getByteBuffer.duplicate()
        buffer.flip()
        // copying a buffer directly is more efficient
        os.write(buffer)
      case _ => super.flush()
    }
  }
}
