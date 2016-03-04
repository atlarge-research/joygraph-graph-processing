package nl.joygraph.core.actor.messaging

import java.io.OutputStream

import com.esotericsoftware.kryo.io.ByteBufferOutput
import nl.joygraph.util.DirectByteBufferGrowingOutputStream

class KryoOutput(bufferSize : Int, maxBufferSize : Int) extends ByteBufferOutput(bufferSize, maxBufferSize) {

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
