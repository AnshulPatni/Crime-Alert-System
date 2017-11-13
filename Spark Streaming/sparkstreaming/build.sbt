
name := "sparkstreaming"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.1"
// libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.3"



assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}