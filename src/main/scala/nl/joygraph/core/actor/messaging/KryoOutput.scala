package nl.joygraph.core.actor.messaging

import com.esotericsoftware.kryo.io.ByteBufferOutput
import nl.joygraph.util.DirectByteBufferGrowingOutputStream

class KryoOutput(bufferSize : Int, maxBufferSize : Int) extends ByteBufferOutput(bufferSize, maxBufferSize) {
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
