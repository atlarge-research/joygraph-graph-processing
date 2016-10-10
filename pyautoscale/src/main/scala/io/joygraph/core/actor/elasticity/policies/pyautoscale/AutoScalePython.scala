package io.joygraph.core.actor.elasticity.policies.pyautoscale

import java.io.{BufferedReader, File, FileOutputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.util.zip.ZipInputStream
import javax.json.{Json, JsonObject}

import com.typesafe.config.Config
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy.{Grow, Result, Shrink}
import io.joygraph.core.actor.metrics.SupplyDemandMetrics
import io.joygraph.core.message.AddressPair
import io.joygraph.core.partitioning.VertexPartitioner
import io.joygraph.core.partitioning.impl.VertexHashPartitioner

import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}

abstract case class AutoScalePython(path : String) extends ElasticPolicy {

  protected[this] val executableName : String
  protected[this] var serverProcess : Process = _
  protected[this] var port : Int = _

  private[this] def copyResources() : String = {
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
    targetDir + "/EaaS"
  }

  override def init(policyParams : Config) : Unit = {
    port = if (policyParams.hasPath("port")) policyParams.getInt("port") else 5012
    val workingDir = policyParams.getString("workingDir")
    val executableLocation = copyResources() + "/" + executableName
    // check if executableLocation exists
    if (!new File(executableLocation).exists()) {
      throw new IllegalArgumentException(s"File $executableLocation does not exist")
    }
    if (!new File(workingDir).exists()) {
      throw new IllegalArgumentException(s"Directory $workingDir does not exist")
    }

    // start process server
    serverProcess = new ProcessBuilder()
      .command("/usr/bin/bash", "-c", s"python2.7 $executableLocation")
      .directory(new File(workingDir))
      .start()
    val br = new BufferedReader(new InputStreamReader(serverProcess.getErrorStream))
    @volatile var foundLine = false

    new Thread(new Runnable {
      override def run(): Unit = {
        Iterator.continually(br.readLine).takeWhile(s => serverProcess.isAlive && s != null).foreach{ line =>
          println("[AutoScalePython]: " + line)
          if (line.contains("Running on")) {
            foundLine = true
          }
        }
      }
    }).start()


    while (serverProcess.isAlive && !foundLine) {
    }

    if (!foundLine) {
      throw new RuntimeException("Process did not start properly")
    }
    sys.addShutdownHook(shutdown())
  }

  def averageServerSpeedNonNormalized(currentStep: Int, currentWorkers: Map[Int, AddressPair]) : Double = {
    val prevAverageRequest = activeVerticesSumOf(currentStep - 1).toDouble / currentWorkers.size.toDouble
    prevAverageRequest.toDouble
  }

  def averageServerSpeedNormalizedByProcessingTime(currentStep : Int, currentWorkers :Map[Int, AddressPair]) : Double = {
    val prevAverageRequest = activeVerticesSumOf(currentStep - 1).toDouble / currentWorkers.size
    val averageTime = averageTimeOfStep(currentStep, currentWorkers.keys)

    // TODO
    ???
  }

  override def decide(currentStep: Int, currentWorkers: Map[Int, AddressPair], currentPartitioner: VertexPartitioner, maxNumWorkers: Int): Option[Result] = {
    val capacity : Int = currentWorkers.size
    val loadRequests : Long = activeVerticesSumOf(currentStep)
    val averageServerSpeed = averageServerSpeedNonNormalized(currentStep, currentWorkers)
    val rawPrediction = predict(averageServerSpeed, capacity, loadRequests)
    val prediction = predictionLimiter(rawPrediction, capacity, maxNumWorkers)
    println(s"time: ${System.currentTimeMillis()}")
    println(s"capacity: $capacity, averageServerSpeed: $averageServerSpeed, loadRequests: $loadRequests")
    println(s"predictionRaw: $rawPrediction, prediction: $prediction")

    // add raw prediction to supply demand
    addRawSupplyDemand(SupplyDemandMetrics(currentStep, System.currentTimeMillis(), capacity, rawPrediction + capacity))
    shrinkOrGrow(prediction, currentWorkers)
  }

  def predictionLimiter(rawPrediction : Int, currentNumWorkers : Int, maxNumWorkers : Int) : Int = {
    if (rawPrediction > 0) {
      Math.min(
        rawPrediction, // maximum should be a little bit different
        Math.max(0, maxNumWorkers - currentNumWorkers) // lowerbound
      )
    } else if (rawPrediction < 0) {
      if (currentNumWorkers + rawPrediction < 1) {
        -(currentNumWorkers - 1)
      } else {
        rawPrediction
      }
    } else {
      0
    }
  }

  def shrinkOrGrow(prediction : Int, currentWorkers : Map[Int, AddressPair]): Option[Result] = {
    val numCurrentWorkers = currentWorkers.size
    if (prediction > 0) {
      // naively add workers
      val workerIds = for(i <- numCurrentWorkers until numCurrentWorkers + prediction) yield i
      Some(Grow(workerIds, new VertexHashPartitioner(workerIds.size + numCurrentWorkers)))
    } else if (prediction < 0) {
      // naively remove workers
      val workerIds = for(i <- numCurrentWorkers + prediction until numCurrentWorkers) yield i
      Some(Shrink(workerIds, new VertexHashPartitioner(currentWorkers.keys.size - workerIds.size)))
    } else {
      None
    }
  }

  def predict(averageServerSpeed : Double, capacity : Int, loadRequests : Long): Int = {
    val jsonRequest = Json.createObjectBuilder()
      .add("server_speed", averageServerSpeed)
      .add("capacity", capacity)
      .add("load_requests", loadRequests)
      .build()

    val response = sendRequest(s"http://localhost:$port/$path", jsonRequest)
    if (response.isNull("prediction")) 0 else response.getJsonNumber("prediction").intValue()
  }

  def shutdown(): Unit = {
    if (serverProcess.isAlive) {
      val field = serverProcess.getClass.getDeclaredField("pid")
      field.setAccessible(true)
      println(s"killing AutoScalePython pid : ${field.get(serverProcess)}")
      serverProcess.destroyForcibly().waitFor()
      Try[Int] {
        serverProcess.exitValue()
      } match {
        case Failure(exception) =>
          println("no exit value")
        case Success(value) =>
          println(value)
      }
    }
  }

  protected[this] def sendRequest(url : String, jsonRequest : JsonObject) : JsonObject = {
    val urlObject = new URL(url)
    val urlConnection = urlObject.openConnection().asInstanceOf[HttpURLConnection]
    urlConnection.setRequestMethod("POST")
    urlConnection.setRequestProperty("Content-Type", "application/json")

    urlConnection.setDoOutput(true)
    Try[Unit] {
      val jsonWriter = Json.createWriter(urlConnection.getOutputStream)
      jsonWriter.write(jsonRequest)
      jsonWriter.close()
    } match {
      case Failure(exception) =>
        println("Send failure, retrying")
        println(exception)
        Thread.sleep(500)
        sendRequest(url, jsonRequest)
      case Success(value) =>
        // noop
    }

    // Get the response
    val res = urlConnection.getResponseCode match {
      case 500 =>
        println("Response failure, retrying")
        Thread.sleep(500)
        sendRequest(url, jsonRequest)
      case x if x > 200 && x < 300 =>
        val jsonReader = Json.createReader(urlConnection.getInputStream)
        val response = jsonReader.readObject()
        jsonReader.close()
        response
      case x @ _ => throw new RuntimeException(s"Other response code $x")
    }
    res
  }
}
