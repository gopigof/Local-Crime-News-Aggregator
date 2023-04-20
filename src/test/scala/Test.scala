import edu.neu.coe.csye7200.crimeagg.util.CsvReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CsvReaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _

  // Create SparkSession before running tests
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("CsvReaderSpec")
      .master("local[*]")
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
    val resultDataFrame = CsvReader.loadCsvToDataFrame(filePath)

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
    val resultDataFrame = CsvReader.loadCsvToDataFrame(filePath, delimiter)

    // Compare schema and data of expected and result DataFrames
    resultDataFrame.schema.fieldNames should contain theSameElementsAs expectedDataFrame.schema.fieldNames
    resultDataFrame.collect() should contain theSameElementsAs expectedDataFrame.collect()
  }
}
