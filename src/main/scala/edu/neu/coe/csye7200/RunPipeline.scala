package edu.neu.coe.csye7200

import edu.neu.coe.csye7200.persistence.ElasticSearchPersistence
import edu.neu.coe.csye7200.util.CsvReader
import org.apache.spark.{SparkConf, SparkContext}

trait SparkContextProvider {
	private val conf = new SparkConf()
		.setMaster("local[*]")
		.setAppName("Pipeline")
		.set("spark.es.nodes", "127.0.0.1:9200")
		.set("spark.es.net.http.auth.user", "elastic")
		.set("spark.es.net.http.auth.pass", "changeme")
		.set("spark.es.index.auto.create", "true")
		.set("spark.es.nodes.wan.only", "true")
	implicit val sparkContext: SparkContext = new SparkContext(conf)
	sparkContext.setLogLevel("WARN")
}

object RunPipeline extends App with SparkContextProvider {

	val rdd = CsvReader.read("C:/Users/gopig/Downloads/Crimes_-_2001_to_Present.csv")
	val sink_ = new ElasticSearchPersistence(rdd)
	sink_.run()
}
