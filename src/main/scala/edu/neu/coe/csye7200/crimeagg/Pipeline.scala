package edu.neu.coe.csye7200.crimeagg

import edu.neu.coe.csye7200.crimeagg.models.BaseRecord
import edu.neu.coe.csye7200.crimeagg.persistence.ElasticSearchPersistence
import edu.neu.coe.csye7200.crimeagg.workflows.Sink
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Pipeline {

	private def applySink(df: DataFrame, sink: Sink): ElasticSearchPersistence =
		sink.`type` match {
			case "ElasticSearch" => new ElasticSearchPersistence(df)
		}

}
