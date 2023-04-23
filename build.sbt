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
val catsVersion = "3.4.9"
val http4sVersion = "0.23.18"

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
	"org.elasticsearch" %% "elasticsearch-hadoop" % "8.7.0",
	// HTTP packages
	"net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.2",
	"commons-net" % "commons-net" % "3.6",
	// Cats and HTTP4s
	"org.typelevel" %% "cats-effect" % catsVersion,
	"org.http4s" %% "http4s-dsl" % http4sVersion,
	"org.http4s" %% "http4s-ember-server" % http4sVersion,
	"org.http4s" %% "http4s-ember-client" % http4sVersion
)

// Execution & Test Options
mainClass in compile := Some("edu.neu.coe.csye7200.Entry")

// Docker Build Options


enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)