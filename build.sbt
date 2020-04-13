name := "Spark"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "junit" % "junit" % "4.4",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "org.mockito" % "mockito-core" % "2.23.4"
)