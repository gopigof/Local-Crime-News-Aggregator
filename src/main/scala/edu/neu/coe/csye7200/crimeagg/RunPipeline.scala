package edu.neu.coe.csye7200.crimeagg

import edu.neu.coe.csye7200.crimeagg.persistence.ElasticSearchPersistence
import edu.neu.coe.csye7200.crimeagg.util.CsvReader.loadCsvToDataFrame
import edu.neu.coe.csye7200.crimeagg.util.SparkContextProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object RunPipeline extends App with SparkContextProvider {


	val chicagoCrimeDf = loadCsvToDataFrame(chicagoCrime)
	val laCrimePastDf = loadCsvToDataFrame(LaCrimePast)
	val laCrimePresentDf = loadCsvToDataFrame(LaCrimePresent)

	val dfCrimeCH = chicagoCrimeDf.na.drop().select(to_timestamp(substring(col("Date"), 1, 19), "MM/dd/yyyy HH:mm:ss").alias("Crime_Time"),
		col("Description").as("Crime_Type"),
		col("Location Description").as("Crime_Loc"),
		col("Latitude").as("Latitude"),
		col("Longitude").as("Longitude")
	).withColumn("Location", concat(col("Latitude"), lit(","), col("Longitude")))
		.select("Crime_Time", "Crime_Type", "Crime_Loc", "Location")

	val dfCrimeLAOld = laCrimePastDf.select(to_date(substring(col("DATE OCC"), 1, 10), "MM/dd/yyyy").as("to_date"),
		col("TIME OCC"),
		col("Crm Cd Desc"),
		col("Premis Desc"),
		col("LAT"),
		col("LON"))

	val dfCrimeLAOld1 = dfCrimeLAOld.na.drop().select(to_timestamp(concat(col("to_date"), lit(" "), lpad(col("TIME OCC").cast(StringType), 4, "0")), "yyyy-MM-dd HHmm").alias("Crime_Time"),
		col("Crm Cd Desc").as("Crime_Type"),
		col("Premis Desc").as("Crime_Loc"),
		col("LAT").as("Latitude"),
		col("LON").as("Longitude")
	).withColumn("Location", concat(col("Latitude"), lit(","), col("Longitude")))
		.select("Crime_Time", "Crime_Type", "Crime_Loc", "Location")


	val dfCrimeLANew = laCrimePresentDf.select(to_date(substring(col("DATE OCC"), 1, 10), "MM/dd/yyyy").as("to_date"),
		col("TIME OCC"),
		col("Crm Cd Desc"),
		col("Premis Desc"),
		col("LAT"),
		col("LON"))

	val dfCrimeLANew1 = dfCrimeLANew.na.drop().select(to_timestamp(concat(col("to_date"), lit(" "), lpad(col("TIME OCC").cast(StringType), 4, "0")), "yyyy-MM-dd HHmm").alias("Crime_Time"),
		col("Crm Cd Desc").as("Crime_Type"),
		col("Premis Desc").as("Crime_Loc"),
		col("LAT").as("Latitude"),
		col("LON").as("Longitude")
	).withColumn("Location", concat(col("Latitude"), lit(","), col("Longitude")))
		.select("Crime_Time", "Crime_Type", "Crime_Loc", "Location")

	dfCrimeCH.printSchema()
	dfCrimeLAOld1.printSchema()
	dfCrimeLANew1.printSchema()

	dfCrimeCH.show()
	dfCrimeLAOld1.show()
	dfCrimeLANew1.show()

	val es = new ElasticSearchPersistence(dfCrimeCH, dfCrimeLANew1, dfCrimeLAOld1)
	es.run()
	sparkSession.stop()
}
