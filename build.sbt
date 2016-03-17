organization := "io.joygraph"
name := "programs"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.0-M3"
publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

val HADOOP_VERSION = "2.7.2"

libraryDependencies ++= Seq(
  "io.joygraph" %% "hadoop" % "0.1-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "3.0.0-M12" % "test",
  "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-hdfs" % HADOOP_VERSION % Test exclude("jline", "jline")
)

// uncomment if skipping tests in assembly task
test in assembly := {}

//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("org.apache.commons.beanutils.**" -> "shade1.beanutils.@1").inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0"),
//  ShadeRule.rename("org.apache.commons.collections.**" -> "shade1.collections.@1").inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0"),
//  ShadeRule.rename("org.apache.commons.collections.**" -> "shade2.collections.@1").inLibrary("commons-beanutils" % "commons-beanutils-core" % "1.8.0")
//)