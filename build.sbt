organization := "com.metamx"

name := "frdy"

scalaVersion in ThisBuild := "2.11.11"

crossScalaVersions := Seq("2.10.6", "2.11.11", "2.12.2")

lazy val root = project.in(file("."))

resolvers ++= Seq(
  "Metamarkets Releases" at "https://metamx.jfrog.io/metamx/libs-releases/"
)

publishMavenStyle := true

publishTo := Some("libs-local" at "https://metamx.jfrog.io/metamx/libs-releases-local")

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.metamx" %% "scala-util" % "1.13.3",
  "net.jpountz.lz4" % "lz4" % "1.3.0",
  "org.xerial.snappy" % "snappy-java" % "1.1.4",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test"
)
