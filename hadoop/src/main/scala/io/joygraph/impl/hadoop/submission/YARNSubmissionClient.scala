package io.joygraph.impl.hadoop.submission

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import io.joygraph.core.config.JobSettings
import io.joygraph.core.submission.SubmissionClient
import io.joygraph.impl.hadoop.util.YARNUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.annotation.tailrec
import scala.collection.JavaConversions._
object YARNSubmissionClient {

  val APPMASTERJARNAME = "appMaster.jar"
  val JOYGRAPHCONFIGNAME = "joygraph.conf"
  val RESOURCEPATHSNAME = "resource.paths"

  class Builder {
    private[this] var _applicationName : String = _
    private[this] var _appMasterJarPath : String = _
    private[this] var _joygraphConfPath : String = _
    private[this] var _amMemory : Int = _
    private[this] var _amVCores : Int = _
    private[this] var _queue : String = _
    private[this] var _yarnClient : YarnClient = _
    private[this] var _fs : FileSystem = _

    def applicationName(applicationName : String) : Builder = {
      _applicationName = applicationName
      this
    }

    def appMasterJarPath(appMasterJarPath : String) : Builder = {
      _appMasterJarPath = appMasterJarPath
      this
    }

    def joygraphConfPath(joygraphConfPath : String) : Builder = {
      _joygraphConfPath = joygraphConfPath
      this
    }

    def amMemory(amMemory : Int) : Builder = {
      _amMemory = amMemory
      this
    }

    def amVCores(amVCores : Int) : Builder = {
      _amVCores = amVCores
      this
    }

    def yarnClient(yarnClient : YarnClient) : Builder = {
      _yarnClient = yarnClient
      this
    }

    def fs(fs : FileSystem) : Builder = {
      _fs = fs
      this
    }

    def queue(queue : String) : Builder = {
      _queue = queue
      this
    }

    def build() : YARNSubmissionClient = {
      // TODO checks
      new YARNSubmissionClient(
        _applicationName,
        _appMasterJarPath,
        _joygraphConfPath,
        _yarnClient,
        _fs,
        _amMemory,
        _amVCores,
        _queue
      )
    }
  }

  def main(args: Array[String]): Unit = {
    val appMasterJarPath = args(0)
    val joygraphConfPath : String = args(1)
    val applicationName = args(2)

    val joyGraphConf = ConfigFactory.parseFile(new File(joygraphConfPath))
    val amMemory = JobSettings(joyGraphConf).masterMemory
    val amVCores = JobSettings(joyGraphConf).masterCores

    // TODO setup configuration
    val conf : Configuration = new HdfsConfiguration()
    val yarnConf : Configuration = new YarnConfiguration()

    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(yarnConf)
    yarnClient.start()
    val fs = FileSystem.get(conf)

    val submissionClient = new Builder()
      .appMasterJarPath(appMasterJarPath)
      .joygraphConfPath(joygraphConfPath)
      .applicationName(applicationName)
      .yarnClient(yarnClient)
      .fs(fs)
      .amMemory(amMemory)
      .amVCores(amVCores)
      .build()

    submissionClient.submit()
  }

}

class YARNSubmissionClient protected(
                                      applicationName : String,
                                      appMasterJarPath : String,
                                      joygraphConfPath : String,
                                      yarnClient : YarnClient,
                                      fs : FileSystem,
                                      amMemory : Int,
                                      amVCores : Int,
                                      queue : String) extends SubmissionClient {

  import YARNSubmissionClient._

  private[this] val localResources : util.Map[String, LocalResource] = new util.HashMap[String, LocalResource]()
  private[this] val env : util.Map[String, String] = new util.HashMap[String, String]()
  private[this] val commands : util.List[String] = new util.ArrayList[String]()

  private[this] def setResources(context : ApplicationSubmissionContext) = {
    val applicationIdString =  context.getApplicationId.toString
    val (appMasterJarDfsPath, appMasterJarLocalResource) = localResource(fs, APPMASTERJARNAME, appMasterJarPath, applicationIdString)
    val (_, jobConfigLocalResource) = localResource(fs, JOYGRAPHCONFIGNAME, joygraphConfPath, applicationIdString)
    val (_, pathsLocalResource) = serializeResourcesFileStatuses(fs, RESOURCEPATHSNAME, Seq(appMasterJarDfsPath), applicationIdString)
    localResources += appMasterJarLocalResource
    localResources += jobConfigLocalResource
    localResources += pathsLocalResource
  }

  private[this] def _submitApplication() : ApplicationId = {
    val application = yarnClient.createApplication()
    val context = application.getApplicationSubmissionContext
    context.setApplicationName(applicationName)
    context.setKeepContainersAcrossApplicationAttempts(false)
    context.setMaxAppAttempts(1) // TODO configurable maybe

    setResources(context)
    env += classPath()
    commands += masterCommand(APPMASTERJARNAME, JOYGRAPHCONFIGNAME, RESOURCEPATHSNAME, amMemory)

    val capability = Resource.newInstance(amMemory, amVCores)
    context.setResource(capability)

    val amContainer = ContainerLaunchContext.newInstance(
      localResources, env, commands, null, null, null)

    context.setAMContainerSpec(amContainer)
    context.setResource(capability)
    context.setQueue(queue)

    yarnClient.submitApplication(context)
  }

  override def submit(): Unit = {
    val appId = _submitApplication()
    // TODO report is unused at the moment as there's no use for it.
    val report = yarnClient.getApplicationReport(appId)
    // submit and forget?
  }

  @tailrec
  private[this] def pollForCompletion(appId: ApplicationId, waitTime : Long = 1000) : FinalApplicationStatus= {
    val report = yarnClient.getApplicationReport(appId)
    if (report.getYarnApplicationState match {
      case yarnAppState @
        (YarnApplicationState.FINISHED |
         YarnApplicationState.FAILED |
         YarnApplicationState.KILLED) =>
        report.getFinalApplicationStatus match {
          case FinalApplicationStatus.UNDEFINED => println("UNDEFINED after YarnApplicationState " + yarnAppState )
          case finalAppStatus @
            (FinalApplicationStatus.SUCCEEDED |
             FinalApplicationStatus.FAILED |
             FinalApplicationStatus.KILLED)=> println("application finished with " + finalAppStatus)
        }
        true
      case _ =>
        false
    }) {
      synchronized{
        wait(waitTime)
      }
      pollForCompletion(appId)
    } else {
      report.getFinalApplicationStatus
    }
  }

  override def submitBlocking(): Unit = {
    val appId = _submitApplication()
    pollForCompletion(appId)
  }
}
