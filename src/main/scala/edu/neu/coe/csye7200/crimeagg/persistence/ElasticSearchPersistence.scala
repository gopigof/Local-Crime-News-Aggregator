package edu.neu.coe.csye7200.crimeagg.persistence

import edu.neu.coe.csye7200.crimeagg.elasticIndexName
import edu.neu.coe.csye7200.crimeagg.models.BaseRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.elasticsearch.spark.sql.{EsSparkSQL, sparkDatasetFunctions}

import scala.util.control.NonFatal

trait PersistenceRDD {
	protected val dfs: Seq[DataFrame]

	def run()(implicit SQLContext: SQLContext): Unit
}


class ElasticSearchPersistence(protected val dfs: DataFrame*) extends PersistenceRDD {
	def run()(implicit sqlContext: SQLContext): Unit = {
		println("Saving dataframe to elastic search")
		dfs.foreach{df =>
			try
				df.saveToEs(elasticIndexName)
			catch {
				case NonFatal(th) =>
					th.printStackTrace()
			}
		}
	}
}
