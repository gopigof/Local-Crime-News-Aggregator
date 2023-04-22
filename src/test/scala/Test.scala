import edu.neu.coe.csye7200.crimeagg.util.CsvReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types.{StringType, TimestampType}

import java.sql.Timestamp


class CsvReaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _

  // Create SparkSession before running tests
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("CsvReaderSpec")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  // Stop SparkSession after running tests
  override def afterAll(): Unit = {
    spark.stop()
  }

  "CsvReader" should "load CSV file to DataFrame" in {
    // Prepare test data
    val filePath = "src/test/resources/test.csv"
    val expectedSchema = Seq("col1", "col2", "col3")
    val expectedData = Seq(("row1_col1", "row1_col2", "row1_col3"),
      ("row2_col1", "row2_col2", "row2_col3"))

    // Create a test DataFrame
    val expectedDataFrame = spark.createDataFrame(expectedData).toDF(expectedSchema: _*)

    // Call the method to load CSV to DataFrame
    val resultDataFrame = CsvReader.loadCsvToDataFrame(filePath)(spark)

    // Compare schema and data of expected and result DataFrames
    resultDataFrame.schema.fieldNames should contain theSameElementsAs expectedDataFrame.schema.fieldNames
    resultDataFrame.collect() should contain theSameElementsAs expectedDataFrame.collect()
  }

  it should "load CSV file to DataFrame with specified delimiter" in {
    // Prepare test data
    val filePath = "src/test/resources/test_pipe_delimited.csv"
    val delimiter = "|"
    val expectedSchema = Seq("col1", "col2", "col3")
    val expectedData = Seq(("row1_col1", "row1_col2", "row1_col3"),
      ("row2_col1", "row2_col2", "row2_col3"))

    // Create a test DataFrame
    val expectedDataFrame = spark.createDataFrame(expectedData).toDF(expectedSchema: _*)

    // Call the method to load CSV to DataFrame with specified delimiter
    val resultDataFrame = CsvReader.loadCsvToDataFrame(filePath, delimiter)(spark)

    // Compare schema and data of expected and result DataFrames
    resultDataFrame.schema.fieldNames should contain theSameElementsAs expectedDataFrame.schema.fieldNames
    resultDataFrame.collect() should contain theSameElementsAs expectedDataFrame.collect()
  }

  // Test case for verifying date and time format LA
  "CsvReader.loadCsvToDataFrame" should "load CSV data with correct date and time format for LA" in {
    // Load CSV data to DataFrame
    val filePath = "src/test/resources/test_date.csv"
    val df = CsvReader.loadCsvToDataFrame(filePath, ",")(spark)

    // Verify date and time format
    val dateAndTimeFormat = "MM/dd/yyyy HH:mm"
    val dateFormatColumn = to_date(substring(col("DATE OCC"), 1, 10), "MM/dd/yyyy").as("to_date")
    val timeFormatColumn = lpad(col("TIME OCC").cast(StringType), 4, "0").as("TIME OCC")
    val formattedDateTime = to_timestamp(concat(dateFormatColumn, lit(" "), timeFormatColumn), "yyyy-MM-dd HHmm")

    // Perform assertions
    val resultDate = df.select(dateFormatColumn).collect()
    resultDate.head.getAs[java.sql.Date]("to_date").toString should be("2020-01-08")

    val resultDateTime = df.select(formattedDateTime.cast(TimestampType)).collect()
    val expectedDateTime = java.sql.Timestamp.valueOf("2020-01-08 22:30:00.0")
    resultDateTime.head.getAs[java.sql.Timestamp](0) should be(expectedDateTime)


  }

  // Test case for verifying date and time format CH
  "CsvReader.loadCsvToDataFrame" should "load CSV data with correct date and time format for CH" in {
    // Load CSV data to DataFrame
    val filePath = "src/test/resources/test_date_CH.csv"
    val df1 = CsvReader.loadCsvToDataFrame(filePath, ",")(spark)

    val df: DataFrame = df1.select(
        to_timestamp(substring(col("Date"), 1, 19), "MM/dd/yyyy HH:mm").alias("Crime_Time")
      )

    // Verify date and time format
    val expectedDateTime1 = java.sql.Timestamp.valueOf("2015-09-05 13:30:00.0")
    val expectedDateTime2 = java.sql.Timestamp.valueOf("2015-09-04 11:30:00.0")
    val expectedDateTime3 = java.sql.Timestamp.valueOf("2018-09-01 00:01:00.0")

    val result1 = df.filter(col("Date") === "9/5/2015 13:30").select("Crime_Time").collect()
    result1.head.getAs[Timestamp]("Crime_Time") should be(expectedDateTime1)

    val result2 = df.filter(col("Date") === "9/4/2015 11:30").select("Crime_Time").collect()
    result2.head.getAs[Timestamp]("Crime_Time") should be(expectedDateTime2)

    val result3 = df.filter(col("Date") === "9/1/2018 0:01").select("Crime_Time").collect()
    result3.head.getAs[Timestamp]("Crime_Time") should be(expectedDateTime3)

  }


}
