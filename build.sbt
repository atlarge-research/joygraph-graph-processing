import sbt.Keys._
import sbt._

val HADOOP_VERSION = "2.7.2"
val AKKA_VERSION = "2.4.4"
val SCALA_VERSION = "2.12.0-M4"

lazy val commonSettings = Seq(
  organization := "io.joygraph",
  version := "0.1-SNAPSHOT",
  scalaVersion := SCALA_VERSION,
  publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
  //  checksums := Seq("")
)

lazy val hadoop = (project in file("hadoop")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION % Provided exclude("jline", "jline"),
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HADOOP_VERSION % Provided exclude("jline", "jline"),
      "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION % Provided exclude("jline", "jline"),
      "org.apache.hadoop" % "hadoop-hdfs" % HADOOP_VERSION % Provided exclude("jline", "jline")
    ) ++ testDependencies ++ hadoopTestDependencies
  ).dependsOn(core)

lazy val cluster = (project in file("cluster"))
  .settings(commonSettings: _*)
  .dependsOn(core)

lazy val core = (project in file("core"))
  .settings(commonSettings: _*)
  .settings(name := "core")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % SCALA_VERSION,
      "com.typesafe.akka" %% "akka-actor" % AKKA_VERSION,
      "com.typesafe.akka" %% "akka-remote" % AKKA_VERSION,
      "com.typesafe.akka" %% "akka-cluster" % AKKA_VERSION,
      "com.typesafe.akka" %% "akka-cluster-metrics" % AKKA_VERSION,
      "com.typesafe" % "config" % "1.3.0",
      "com.esotericsoftware" % "kryo-shaded" % "3.0.3",
      "io.netty" % "netty-all" % "4.0.34.Final"
      //  "net.openhft" % "chronicle-map" % "3.4.2-beta"
    ))
  .settings(
    libraryDependencies ++= testDependencies
  )
  .settings(
    scalacOptions += "-feature"
  )

// disable publishing of root project using publish to mvn
lazy val joygraph = (project in file("."))
  .settings(commonSettings : _*)
  .settings(
    publish := {}
  )
  .aggregate(core, cluster, hadoop)

lazy val hadoopTestDependencies = Seq(
  "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-hdfs" % HADOOP_VERSION % Test exclude("jline", "jline") classifier "tests",
  "org.apache.hadoop" % "hadoop-yarn-server-tests" % HADOOP_VERSION % Test classifier "tests"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.0-M16-SNAP4" % "test"
)
