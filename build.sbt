import sbt.Keys._
import sbt._

val HADOOP_VERSION = "2.7.2"
val AKKA_VERSION = "2.5.4"
val SCALA_VERSION = "2.12.3"

lazy val commonSettings = Seq(
  organization := "io.joygraph",
  version := "0.2-SNAPSHOT",
  scalaVersion := SCALA_VERSION,
  publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
  //  checksums := Seq("")
)

lazy val pyautoscale = (project in file("pyautoscale")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "javax.json" % "javax.json-api" % "1.0",
      "org.glassfish" % "javax.json" % "1.0.4"
    ) ++ testDependencies
  ).
  dependsOn(core)

lazy val analysis = (project in file("analysis")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-math3" % "3.6.1"
    ) ++ testDependencies
  ).
  dependsOn(core)

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
      "io.netty" % "netty-all" % "4.0.34.Final",
      "io.kamon" % "sigar-loader" % "1.6.6-rev002", // hyperic sigar for enhanced CPU metrics
      "org.caffinitas.ohc" % "ohc-core" % "0.6.1",
      "org.caffinitas.ohc" % "ohc-core-j8" % "0.6.1"
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
  .aggregate(core, cluster, hadoop, analysis, pyautoscale)

lazy val hadoopTestDependencies = Seq(
  "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION % Test exclude("jline", "jline"),
  "org.apache.hadoop" % "hadoop-hdfs" % HADOOP_VERSION % Test exclude("jline", "jline") classifier "tests",
  "org.apache.hadoop" % "hadoop-yarn-server-tests" % HADOOP_VERSION % Test classifier "tests"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.2" % "test"
)
