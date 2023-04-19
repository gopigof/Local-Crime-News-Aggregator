package edu.neu.coe.csye7200.persistence

import edu.neu.coe.csye7200.models.BaseRecord
import org.apache.spark.rdd.RDD

import org.elasticsearch.spark._

trait PersistenceRDD {
	protected val rdd: RDD[BaseRecord]

	def run(): Unit
}


class ElasticSearchPersistence(protected val rdd: RDD[BaseRecord]) extends PersistenceRDD {
	def run(): Unit = {
		rdd.saveToEs("test_index")
	}
}
