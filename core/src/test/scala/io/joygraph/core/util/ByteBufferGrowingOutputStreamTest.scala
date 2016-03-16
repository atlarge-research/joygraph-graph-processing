package io.joygraph.core.util

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

class ByteBufferGrowingOutputStreamTest extends FunSuite {

  private def time(x : => Any ): Unit = {
    val start = System.currentTimeMillis()

    x

    println(System.currentTimeMillis() - start)
  }

  test("test write buffer") {
    val bboutput = new DirectByteBufferGrowingOutputStream(2)
    val boutputStream = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(boutputStream)
    for(i <- 1 to 1000000) {
      dataOutputStream.writeInt(i)
    }
    dataOutputStream.flush()

    val data = ByteBuffer.wrap(boutputStream.toByteArray)
    bboutput.write(data)
    val duplicate = bboutput.getBuf.duplicate()
    duplicate.flip()
    val a = new com.esotericsoftware.kryo.io.ByteBufferInputStream(duplicate) {
      override def read() = {
        super.read() & 0xFF
      }
    }
    val dataInputStream = new DataInputStream(a)
    for(i <- 1 to 1000000) {
      Try {
        val res = dataInputStream.readInt()
        assertResult(i)(res)
      } match {
        case Failure(exception) =>
          println(i)
        case Success(value) =>
      }
    }
  }


// TODO replace with another test as writeInt is not supported anymore
//  test("DirectBuffer write and read") {
//    val boutputStream = new DirectByteBufferGrowingOutputStream(32)
//    val numElements = 1000000
//    time {
//      val dataOutputStream = new DataOutputStream(boutputStream)
//      for(i <- 1 to numElements) {
//        dataOutputStream.writeInt(i)
//      }
//      dataOutputStream.flush()
//    }
//    val dup = boutputStream.getBuf.duplicate()
//    dup.flip()
//    val a = new com.esotericsoftware.kryo.io.ByteBufferInputStream(dup) {
//      override def read() = {
//        super.read() & 0xFF
//      }
//    }
//    time {
//      val dataInputStream = new DataInputStream(a)
//      for (i <- 1 to numElements) {
//        assertResult(i)(dataInputStream.readInt())
//      }
//    }
//  }


//  test("ByteArray write and read") {
//    val boutputStream = new ByteArrayOutputStream()
//    time() {
//      val dataOutputStream = new DataOutputStream(boutputStream)
//      for(i <- 1 to 100000000) {
//        dataOutputStream.writeInt(i)
//      }
//      dataOutputStream.flush()
//    }
//
//    val dataInputStream = new DataInputStream(new ByteArrayInputStream(boutputStream.toByteArray))
//    time() {
//      for (i <- 1 to 100000000) {
//        assertResult(i)(dataInputStream.readInt())
//      }
//    }
//  }

}
