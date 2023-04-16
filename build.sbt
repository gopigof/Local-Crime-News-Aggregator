name := "CSYE7200_FinalProject"

version := "1.0"

scalaVersion := "2.13.10"

scalacOptions ++= Seq(
	"-encoding", "utf-8",
	"-deprecation",
	"-Ywarn-dead-code",
	"-Ywarn-value-discard",
	"-Ywarn-unused"
)
val sparkVersion = "3.3.1"

// Dependencies
libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "3.2.14" % "test",
	"org.apache.spark" %% "spark-sql" % sparkVersion,
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-mllib" % sparkVersion
)
// Execution & Test Options
mainClass in compile := Some("edu.neu.coe.csye7200.Entry")

// Docker Build Options


enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)