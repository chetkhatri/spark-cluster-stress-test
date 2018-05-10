import sbt.ExclusionRule
name := "spark-cluster-stress-test"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.2.0" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.7"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
