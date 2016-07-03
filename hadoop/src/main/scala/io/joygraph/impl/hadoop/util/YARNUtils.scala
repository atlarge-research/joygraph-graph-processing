package io.joygraph.impl.hadoop.util

import java.io._

import io.joygraph.impl.hadoop.actor.{YarnAMBaseActor, YarnBaseActor}
import org.apache.hadoop.fs.{FileStatus, FileSystem, FsUrlStreamHandlerFactory, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration

object YARNUtils {

  {
    // Adding the hdfs protocol to java.net.URL
    java.net.URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
  }

  def defaultPriority : Priority = {
    Priority.newInstance(1)
  }

  def newPriority(priority : Int) : Priority = {
    Priority.newInstance(priority)
  }

  /**
    * MaxCapability returned by ApplicationResponse is not guaranteed to be correct (2.7.2),
    * there may be discrepancies between different properties in YARN, namely
    * yarn.scheduler.maximum-allocation-vcores
    * yarn.nodemanager.resource.cpu-vcores
    * The cap returned is maximum-allocation-vcores rather than resource.cpu-vcores.
    * However when submitted, YARN checks the request for both properties and may reject your request
    * @return
    */
  def cappedResource(maxResource : Resource, memory : Int, vCores : Int): Resource = {
    println("MAX RESOURCE: " + maxResource)
    val maxNumCores = math.min(maxResource.getVirtualCores, vCores)
    val maxMemory = math.min(maxResource.getMemory, memory)
    Resource.newInstance(maxMemory, maxNumCores)
  }

  /**
    * Copies local `fileSrcPath` to `{fs.getHomeDirectory}/{appId}_{fileName}`, symbolically links `fileName` for
    * application master.
    *
    * @return `DFSPath` -> (`fileName`, [[org.apache.hadoop.yarn.api.records.LocalResource]]) pair
    */
  def localResource(fs : FileSystem, fileName : String, fileSrcPath: String, dstFileNamePrefix : String) : (Path, (String, LocalResource)) = {
    val srcPath = strToPath(fileSrcPath)
    val dstPath = new Path(fs.getHomeDirectory, s"${dstFileNamePrefix}_$fileName")
    fs.copyFromLocalFile(false, true, srcPath, dstPath)
    val fileStatus = fs.getFileStatus(dstPath)

    localResource(fileName, fileStatus)
  }

  def localResource(fileName : String, fileStatus : FileStatus): (Path, (String, LocalResource)) = {
    val dstPath = fileStatus.getPath
    dstPath -> (fileName -> LocalResource.newInstance(
      pathToSerializableURL(dstPath),
      LocalResourceType.FILE,
      LocalResourceVisibility.APPLICATION,
      fileStatus.getLen,
      fileStatus.getModificationTime
    ))
  }

  private[this] def strToPath(path : String) : Path = new Path(path)
  private[this] def pathToSerializableURL(path : Path) : URL = {
    val javaUrl = path.toUri.toURL
    URL.newInstance(javaUrl.getProtocol, javaUrl.getHost, javaUrl.getPort, javaUrl.getPath)
  }

  def serializeResourcesFileStatuses(fs : FileSystem, resourceName : String, paths : Seq[Path], appId : String) : (Path, (String, LocalResource)) = {
    val fileStatuses = paths.map(path => fs.getFileStatus(path))
    val file = File.createTempFile(appId+"_", ".paths")
    val dos = new DataOutputStream(new FileOutputStream(file))
    dos.writeInt(fileStatuses.size)
    fileStatuses.foreach(_.write(dos))
    dos.flush()
    dos.close()

    localResource(fs, resourceName, file.getAbsolutePath, appId)
  }

  def deserializeResourcesFileStatuses(resourceName : String) : Seq[FileStatus] = {
    val dis = new DataInputStream(new FileInputStream(new File(resourceName)))
    val numPaths = dis.readInt()
    val paths = for (i <- 0 until numPaths) yield {
      val fileStatus = new FileStatus()
      fileStatus.readFields(dis)
      fileStatus
    }
    dis.close()
    paths
  }

  // TODO configurable
  val heapFraction = 0.5
  val directMemoryFraction = 0.5

  def classPath() : (String, String) = {
    // set up classpath, this will just be the fat appMaster.jar ... and the $CLASSPATH which already contains
    // Hadoop related things.
    val conf = new YarnConfiguration()
    val hadoopPaths =
      conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH : _*)
      .reduce(_ + ApplicationConstants.CLASS_PATH_SEPARATOR + _)

    // assume classpath is ApplicationConstants.Environment.HADOOP_YARN_HOME
    val mapreducePaths =
      ApplicationConstants.Environment.HADOOP_YARN_HOME.$$() + "/share/hadoop/mapreduce/*" +
        ApplicationConstants.CLASS_PATH_SEPARATOR +
        ApplicationConstants.Environment.HADOOP_YARN_HOME + "/share/hadoop/mapreduce/lib/*"

    // we've included the HADOOP and MR libs here
    val classPathEnv = Seq(
      Environment.CLASSPATH.$$(),
      hadoopPaths,
      mapreducePaths,
      "./*")
      .reduce(_ + ApplicationConstants.CLASS_PATH_SEPARATOR + _)

    println(classPathEnv)
    ("CLASSPATH", classPathEnv)
  }

  def masterCommand(jarName : String, configName : String, resourcePathsNames : String, memory : Int) : String = {
    val heapMemory : Int = (memory * heapFraction).toInt
    val directMemory : Int = (memory * directMemoryFraction).toInt
    Seq(
      ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java",
      s"-Xms${heapMemory}m",
      s"-Xmx${heapMemory}m",
      s"-XX:MaxDirectMemorySize=${directMemory}m",
//      jarName, // the jar is redundant as it's in the classpath
      classOf[YarnAMBaseActor].getName, // the class
      configName, // args
      resourcePathsNames, // args
      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + s"/$jarName.stdout",
      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + s"/$jarName.stderr"
    ).reduce(_ + " " + _)
  }

  def workerCommand(jarName : String, configName : String, memory : Int) : String = {
    val heapMemory : Int = (memory * heapFraction).toInt
    val directMemory : Int = (memory * directMemoryFraction).toInt
    Seq(
      ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java",
      s"-Xms${heapMemory}m",
      s"-Xmx${heapMemory}m",
      s"-XX:MaxDirectMemorySize=${directMemory}m",
      //      jarName, // the jar is redundant as it's in the classpath
      classOf[YarnBaseActor].getName, // the class
      configName, // args
      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + s"/$jarName.stdout",
      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + s"/$jarName.stderr"
    ).reduce(_ + " " + _)
  }

}
