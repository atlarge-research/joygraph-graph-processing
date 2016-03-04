package nl.joygraph.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.ByteBufferInput
import nl.joygraph.core.actor.messaging.{KryoOutput, ReusableIterable}
import org.scalatest.FunSuite

class ReusableIterableTest extends FunSuite {
  test("testReusableIterableRead") {
    val reusableIterable = new ReusableIterable[Long]{
      override protected[this] def deserializeObject(): Long = _kryo.readObject(_input, classOf[Long])
    }

    val kryo : Kryo = new Kryo()
    val input = new ByteBufferInput(4096)

    // create data
    val byteBufferOutputStream = new DirectByteBufferGrowingOutputStream(0)
    val output = new KryoOutput(4096, 4096)

    for (i <- 1L to 10000L) {
      output.setOutputStream(byteBufferOutputStream)
      kryo.writeObject(output, i)
      output.flush()
      byteBufferOutputStream.trim()
    }

    val dup = byteBufferOutputStream.getBuf
    dup.flip()
    reusableIterable.input(input)
    reusableIterable.kryo(kryo)
    reusableIterable.buffer(dup)

    assertResult((10000L * (10000L + 1L)) / 2)(reusableIterable.sum)
  }
}
