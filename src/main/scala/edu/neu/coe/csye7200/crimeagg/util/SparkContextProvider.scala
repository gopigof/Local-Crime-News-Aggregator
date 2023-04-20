package edu.neu.coe.csye7200.crimeagg.util

import edu.neu.coe.csye7200.crimeagg.{elasticMasterAuthPass, elasticMasterAuthUser, elasticMasterUri}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

trait SparkContextProvider {
	private val conf = new SparkConf()
		.setMaster("local[*]")
		.setAppName("Pipeline")
		.set("spark.es.nodes", elasticMasterUri)
		.set("spark.es.net.http.auth.user", elasticMasterAuthUser)
		.set("spark.es.net.http.auth.pass", elasticMasterAuthPass)
		//		.set("spark.es.net.ssl", "true")
		//		.set("spark.es.net.ssl.cert.allow.self.signed", "true")
		//		.set("spark.es.net.ssl.keystore.location", "resources/certs")
		//		.set("spark.es.net.ssl.keystore.location", "resources\\certs\\elastic.keystore")
		//		.set("spark.es.net.ssl.keystore.pass", "")
		.set("spark.es.nodes.discovery", "false")
		.set("spark.es.index.auto.create", "true")
		.set("spark.es.nodes.wan.only", "true")

	implicit val sparkContext: SparkContext = new SparkContext(conf)
	implicit val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
	implicit val sparkSQLContext: SQLContext = sparkSession.sqlContext
	sparkContext.setLogLevel("WARN")
}