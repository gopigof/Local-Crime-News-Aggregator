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

// Dependencies
libraryDependencies ++= Seq(
)

// Execution & Test Options
mainClass in compile := Some("edu.neu.coe.csye7200.Entry")

// Docker Build Options


enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)