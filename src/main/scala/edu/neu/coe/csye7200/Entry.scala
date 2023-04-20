package edu.neu.coe.csye7200

import edu.neu.coe.csye7200.util.CsvReader._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object Entry extends App {
	println("This works!")

	val crimeCHDF =  loadCsvToDataFrame("D:/Scala2/CSYE7200_FinalProject/src/test/resources/Crimes_CH_2001_to_Present.csv")
//	val c1 = crimeCHDF.filter(col("Longitude").isNull).count()
//	println(s"befor dropping nulls: $c1")

	val dfCrimeCH = crimeCHDF.na.drop().select(to_timestamp(substring(col("Date"), 1, 19), "MM/dd/yyyy HH:mm:ss").alias("Crime_Time"),
			col("Description").as("Crime_Type"),
			col("Location Description").as("Crime_Loc"),
			col("Latitude").as("Latitude"),
			col("Longitude").as("Longitude")
		)

	//dfCrimeCH.show(20)

	val crimeLADFNew = loadCsvToDataFrame("D:/Scala2/CSYE7200_FinalProject/src/test/resources/Crime_Data_LA_2020_to_Present.csv")
	val crimeLADFOld = loadCsvToDataFrame("D:/Scala2/CSYE7200_FinalProject/src/test/resources/Crime_Data_LA_from_2010_to_2019.csv")

	val dfCrimeLAOld = crimeLADFOld.select(to_date(substring(col("DATE OCC"), 1, 10), "MM/dd/yyyy").as("to_date"),
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
		)

	val dfCrimeLANew = crimeLADFNew.select(to_date(substring(col("DATE OCC"), 1, 10), "MM/dd/yyyy").as("to_date"),
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
	)


	//val c2 = dfCrimeCH.filter(col("Longitude").isNull).count()
	//println(s"after dropping nulls: $c2")
	dfCrimeLANew1.show(10)
	dfCrimeLAOld1.show(10)
	dfCrimeCH.show(10)
	dfCrimeCH.printSchema()
	dfCrimeLAOld1.printSchema()
	dfCrimeLANew1.printSchema()

}