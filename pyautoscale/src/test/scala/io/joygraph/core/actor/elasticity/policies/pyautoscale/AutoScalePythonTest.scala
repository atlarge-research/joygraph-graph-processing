package io.joygraph.core.actor.elasticity.policies.pyautoscale

import java.io.FileOutputStream
import java.util.zip.ZipInputStream

import com.typesafe.config.ConfigFactory
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
