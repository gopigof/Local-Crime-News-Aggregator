package edu.neu.coe.csye7200.util

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import edu.neu.coe.csye7200.models.{BaseRecord, StatusError}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object CsvReader {
	def read(uri: String, delimiter: Char = ',')(implicit sc: SparkContext): RDD[BaseRecord] = {
		val headers = sc.textFile(uri).first()
		sc.textFile(uri)
			.mapPartitionsWithIndex {
				case (index, itr) =>
					if (index == 0)
						readFile(itr.drop(1), headers, delimiter)
					else
						readFile(itr, headers, delimiter)
			}
	}

	private def readFile(itr: Iterator[String], firstLine: String, delimiter: Char): Iterator[BaseRecord] = {
		val parser = new CsvFileParser(delimiter)
		val headers = parser.parse(firstLine)

		itr.map { line =>
			Try(parser.parse(line)) match {
				case Success(row) =>
					if (row.length == headers.length)
						BaseRecord(headers.zip(row).toMap)
					else
						BaseRecord(Map("record" -> line), StatusError, Some("Headers mismatch"))
				case Failure(exception) =>
					exception.printStackTrace()
					BaseRecord(Map("record" -> line), StatusError, Some("Invalid Record"))
			}
		}
	}
}


class CsvFileParser(delimiter: Char = ',') {
	private val settings = new CsvParserSettings
	private val format = settings.getFormat
	format.setDelimiter(delimiter)
	format.setComment('\0')
	settings.setIgnoreLeadingWhitespaces(true)
	settings.setIgnoreTrailingWhitespaces(true)
	settings.setNullValue("")
	settings.setMaxCharsPerColumn(-1)
	settings.setMaxColumns(20000)

	private val parser = new CsvParser(settings)

	def parse(line: String): Array[String] =
		parser.parseLine(line)
}