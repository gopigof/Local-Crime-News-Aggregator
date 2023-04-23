package edu.neu.coe.csye7200

package object crimeagg {
	val elasticMasterUri: String = "a622c440d4ef942209eff82d6a9f5694-1880053294.us-east-2.elb.amazonaws.com:9200"
	val elasticMasterAuthUser: String = "elastic"
	val elasticMasterAuthPass: String = "744IQ5DUOy6p1na90Zd7KU5U"
	// Local Elastic config
	val elasticLocalMasterUri: String = "localhost:9200"
	val elasticLocalMasterAuthPass: String = "changeme"

	val elasticIndexName: String = "crime-index-1"

	val resourceRoot: String = ""
	val chicagoCrime: String = resourceRoot + "\\Crimes_-_2001_to_Present.csv"
	val LaCrimePast: String = resourceRoot + "\\Crime_Data_from_2010_to_2019.csv"
	val LaCrimePresent: String = resourceRoot + "\\Crime_Data_from_2020_to_Present.csv"
}
