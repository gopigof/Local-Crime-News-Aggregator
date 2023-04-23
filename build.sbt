name := "CSYE7200_FinalProject"

version := "1.0"

scalaVersion := "2.12.17"

scalacOptions ++= Seq(
	"-encoding", "utf-8",
	"-deprecation",
//	"-Ywarn-dead-code",
//	"-Ywarn-value-discard",
//	"-Ywarn-unused"
)
val sparkVersion = "3.3.1"

//val http4sVersion = "0.23.19-RC3"


// Dependencies
libraryDependencies ++= Seq(
	// Scala Test
	"org.scalatest" %% "scalatest" % "3.2.14" % "test",
	// CSV Parser
	"com.univocity" % "univocity-parsers" % "2.9.1",
	// Apache Spark
	"org.apache.spark" %% "spark-sql" % sparkVersion,
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-mllib" % sparkVersion,
	// ElasticSearch
	"org.elasticsearch" %% "elasticsearch-spark-30" % "8.7.0",
  "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.2",
  "commons-net" % "commons-net" % "3.6",
	"org.typelevel" %% "cats-effect" % "3.4.9",
	"org.http4s" %% "http4s-dsl" % "0.23.19-RC3",
	"org.http4s" %% "http4s-ember-server" % "0.23.19-RC3",
	"org.http4s" %% "http4s-ember-client" % "0.23.19-RC3"
)

// Execution & Test Options
mainClass in compile := Some("edu.neu.coe.csye7200.Entry")

// Docker Build Options


enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)