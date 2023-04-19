package edu.neu.coe.csye7200

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object Entry extends App {
	println("This works!")

	val spark = SparkSession.builder()
		.appName("Crime Rate")
		.master("local[*]")
		.getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")

	val crimeLADF = spark.read.format("csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.load("D:/Scala2/CSYE7200_FinalProject/src/test/resources/Crime_Data_LA_2020_to_Present.csv")

	//val colToKeep = Array("DATE OCC","TIME OCC","Crm Cd Desc","Premis Desc","LAT","LON")
	val dfCrimeLA = crimeLADF.select(to_date(substring(col("DATE OCC"), 1, 10), "MM/dd/yyyy").as("to_date"),
		col("TIME OCC"),
		col("Crm Cd Desc"),
		col("Premis Desc"),
		col("LAT"),
		col("LON"))

	dfCrimeLA.select(concat(col("to_date"), lit(" "), lpad(col("TIME OCC").cast(StringType), 4, "0"))).show(10)
	val dfCrimeLA1 = dfCrimeLA.select(to_timestamp(concat(col("to_date"), lit(" "), lpad(col("TIME OCC").cast(StringType), 4, "0")), "yyyy-MM-dd HHmm").alias("Crime_Time"),
		col("Crm Cd Desc").as("Crime_Type"),
		col("Premis Desc").as("Crime_Loc"),
		col("LAT").as("Latitude"),
		col("LON").as("Longitude")
	)
	println("**********************LA from 2020 to present******************************")
	dfCrimeLA1.show(20)
	dfCrimeLA1.printSchema()

	val crimeCHDF = spark.read.format("csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.load("D:/Scala2/CSYE7200_FinalProject/src/test/resources/Crimes_CH_2001_to_Present.csv")


	val dfCrimeCH = crimeCHDF.select(to_timestamp(substring(col("Date"), 1, 19), "MM/dd/yyyy HH:mm:ss").alias("Crime_Time"),
		col("Description").as("Crime_Type"),
		col("Location Description").as("Crime_Loc"),
		col("Latitude").as("Latitude"),
		col("Longitude").as("Longitude")
	)
	println("**********************************Chicago from 2001 to present********************************************")
	dfCrimeCH.show(20)
	dfCrimeCH.write.format("es")
	dfCrimeCH.printSchema()


}