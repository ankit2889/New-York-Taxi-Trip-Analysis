name := """play-scala"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)


scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test
)

libraryDependencies += "com.typesafe.play" % "play-test_2.11" % "2.5.2"
libraryDependencies += "com.esri.geometry" % "esri-geometry-api" % "1.2.1"
libraryDependencies += "io.spray" % "spray-json_2.11" % "1.3.2"
libraryDependencies += "com.github.nscala-time" % "nscala-time_2.10" % "0.2.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.7.3"


resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"


fork in run := true