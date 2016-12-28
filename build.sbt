name := """SimpleCrawler"""

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.2",
  "org.jsoup" % "jsoup" % "1.9.2"
)

