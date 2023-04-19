package edu.neu.coe.csye7200

import edu.neu.coe.csye7200.models.BaseRecord
import edu.neu.coe.csye7200.persistence.ElasticSearchPersistence
import edu.neu.coe.csye7200.workflows.Sink
import org.apache.spark.rdd.RDD

object Pipeline {

	private def applySink(rdd: RDD[BaseRecord], sink: Sink): ElasticSearchPersistence =
		sink.`type` match {
			case "ElasticSearch" => new ElasticSearchPersistence(rdd)
		}

}
