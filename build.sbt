import sbt.Keys._
import sbt._

lazy val commonSettings = Seq(
  organization := "io.joygraph",
  version := "0.1.0",
  scalaVersion := "2.12.0-M3"
//  checksums := Seq("")
)

lazy val programs = (project in file("programs")).
  settings(commonSettings: _*).
  settings(
    // other settings
  ).dependsOn(joygraph)

lazy val run = (project in file("run")).
  settings(commonSettings: _*).
  settings(
    // other settings
    libraryDependencies ++= testDependencies
  ).dependsOn(joygraph)
  .dependsOn(hadoop).dependsOn(programs)

lazy val hadoop = (project in file("hadoop")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1" exclude("jline", "jline"),
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.1" exclude("jline", "jline"),
      "org.apache.hadoop" % "hadoop-common" % "2.7.1" exclude("jline", "jline")
    )
  ).dependsOn(joygraph)


lazy val joygraph = (project in file("."))
  .settings(commonSettings: _*)


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.12.0-M3",
  "it.unimi.dsi" % "fastutil" % "7.0.10",
  "com.typesafe.akka" %% "akka-actor" % "2.4.1",
  "com.typesafe.akka" %% "akka-remote" % "2.4.1",
  "com.typesafe.akka" %% "akka-cluster" % "2.4.1",
  "com.typesafe" % "config" % "1.3.0",
  "com.esotericsoftware" % "kryo-shaded" % "3.0.3",
  "io.netty" % "netty-all" % "4.0.34.Final"
//  "net.openhft" % "chronicle-map" % "3.4.2-beta"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.0-M12" % "test"
)

libraryDependencies ++= testDependencies