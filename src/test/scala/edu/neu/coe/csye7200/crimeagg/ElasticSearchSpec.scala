package edu.neu.coe.csye7200.crimeagg


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor
import org.elasticsearch.hadoop.util.TestSettings
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ElasticSearchSpec extends AnyFlatSpec with Matchers {
	"ElasticSearch" should "be able to extract the field while pushing" in {
		val settings = new TestSettings()
		settings.setProperty(ConstantFieldExtractor.PROPERTY, "test")
		val extractor = new DataFrameFieldExtractor()
		extractor.setSettings(settings)

		val data = (Row("value1", "value2", "target"), StructType(Seq(StructField("foo", StringType), StructField("bar", StringType), StructField("test", StringType))))
		val expected = "target"
		val actual = extractor.field(data)

		assertEquals(expected, actual)
	}

	"ElasticSearch" should "be able to extract nested row value" in {
		val settings = new TestSettings()
		settings.setProperty(ConstantFieldExtractor.PROPERTY, "test.test")
		val extractor = new DataFrameFieldExtractor()
		extractor.setSettings(settings)

		val data = (Row("value1", "value2", Row("target")),
			StructType(Seq(
				StructField("foo", StringType),
				StructField("bar", StringType),
				StructField("test", StructType(Seq(
					StructField("test", StringType)
				)))
			)))
		val expected = "target"
		val actual = extractor.field(data)

		assertEquals(expected, actual)
	}
}