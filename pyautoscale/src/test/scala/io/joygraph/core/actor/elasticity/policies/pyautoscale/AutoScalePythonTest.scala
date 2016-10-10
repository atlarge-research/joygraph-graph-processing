package io.joygraph.core.actor.elasticity.policies.pyautoscale

import java.io.FileOutputStream
import java.util.zip.ZipInputStream

import com.typesafe.config.{Config, ConfigFactory}
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Shrink}
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.impl.VertexHashPartitioner
import org.scalatest.FunSuite

import scala.reflect.io.Directory

// tests will fail if python2.7 is not installed with statsmodels and flask
class AutoScalePythonTest extends FunSuite {
  val config = ConfigFactory.parseString(
    s"""
      | workingDir = ${Directory.makeTemp().jfile.getAbsolutePath}
    """.stripMargin)

  test("copy zip") {
    val resourceStream = getClass.getResourceAsStream("/EaaS.zip")
    val targetDir = Directory.makeTemp().jfile.getAbsolutePath
    val zipStream = new ZipInputStream(resourceStream)
    Iterator.continually(zipStream.getNextEntry).takeWhile(_ != null).foreach{ entry =>
      val targetName = targetDir + "/" + entry.getName
      if (entry.isDirectory) {
        Directory(targetName).createDirectory()
      } else {
        val fout = new FileOutputStream(targetName)
        Iterator.continually(zipStream.read()).takeWhile(_ != -1).foreach(fout.write)
        fout.close()
      }
      zipStream.closeEntry()
    }
  }

  test("Prediction limiter should limit the boundaries of a job") {
    val autoScalePython = new AutoScalePython("") {
      override protected[this] val executableName: String = "test"

      override def init(policyParams: Config): Unit = ""
    }
    assertResult(-9)(autoScalePython.predictionLimiter(-100, 10, 20))
    assertResult(-9)(autoScalePython.predictionLimiter(-10, 10, 20))
    assertResult(-9)(autoScalePython.predictionLimiter(-9, 10, 20))

    assertResult(10)(autoScalePython.predictionLimiter(10, 10, 20))
    assertResult(10)(autoScalePython.predictionLimiter(100, 10, 20))
    assertResult(10)(autoScalePython.predictionLimiter(1000, 10, 20))

    assertResult(0)(autoScalePython.predictionLimiter(0, 10, 20))

    // test withinrange
    for (i <- 1 to 9) {
      assertResult(i)(autoScalePython.predictionLimiter(i, 10, 20))
    }

    for (i <- -9 to -1) {
      assertResult(i)(autoScalePython.predictionLimiter(i, 10, 20))
    }
  }

  test("Partitioning after shrinking should work") {
    val autoScalePython = new AutoScalePython("") {
      override protected[this] val executableName: String = "test"

      override def init(policyParams: Config): Unit = ""
    }

    val currentWorkers: Map[Int, AddressPair] = (for(i <- 0 until 10) yield i -> AddressPair(null, null)).toMap
    autoScalePython.shrinkOrGrow(-9, currentWorkers) match {
      case Some(x) =>
        x match {
          case Shrink(workersToRemove, partitioner) =>
            assertResult(9)(workersToRemove.size)
            assertResult(1)(partitioner.asInstanceOf[VertexHashPartitioner].numWorkers)
            assertResult(1 until 10)(workersToRemove)
          case Grow(workersToAdd, partitioner) =>
            fail("may not be growing")
        }
      case None =>
        fail("may not be none")
    }
  }

  test("Partitioning after growing should work") {
    val autoScalePython = new AutoScalePython("") {
      override protected[this] val executableName: String = "test"

      override def init(policyParams: Config): Unit = ""
    }

    val currentWorkers: Map[Int, AddressPair] = (for(i <- 0 until 10) yield i -> AddressPair(null, null)).toMap
    autoScalePython.shrinkOrGrow(10, currentWorkers) match {
      case Some(x) =>
        x match {
          case Shrink(workersToRemove, partitioner) =>
            fail("may not be shrinking")
          case Grow(workersToAdd, partitioner) =>
            assertResult(10)(workersToAdd.size)
            assertResult(20)(partitioner.asInstanceOf[VertexHashPartitioner].numWorkers)
            assertResult(10 until 20)(workersToAdd)
        }
      case None =>
        fail("may not be none")
    }
  }

  test("React") {
    val react = new React()

    react.init(config)
    println(react.predict(2.0, 4, 500))
    react.shutdown()
  }

  test("AKTE") {
    val akte = new AKTE()
    akte.init(config)
    println(akte.predict(2.0, 4, 500))
    akte.shutdown()
  }

  test("Hist") {
    val hist = new Hist()
    hist.init(config)
    println(hist.predict(2.0, 4, 500))
    hist.shutdown()
  }

  test("Reg") {
    val reg = new Reg()
    reg.init(config)
    println(reg.predict(2.0, 4, 500))
    reg.shutdown()
  }

  test("ConPaaS") {
    val con = new ConPaaS()
    con.init(config)
    println(con.predict(2.0, 4, 500))
    con.shutdown()
  }
}
