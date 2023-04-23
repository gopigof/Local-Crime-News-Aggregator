package edu.neu.coe.csye7200.crimeagg.fetch

import sun.net.www.protocol.http.HttpURLConnection

import java.io.File
import java.net.URL

object CrimeoMeter {
	def getCrimes(startDate: String, endDate: String, fileName: String, state: String, token: String): Unit = {
		val url = new URL(s"https://crimeometer.p.rapidapi.com/stats/?country=US&state=$state&requestToken=$token&start_date=$startDate&end_date=$endDate")

		val connection = url.openConnection().asInstanceOf[HttpURLConnection]
		connection.setConnectTimeout(10 * 1000) //ms
		connection.setReadTimeout(5000)
		connection.connect()

		connection.getResponseCode match {
			case x if x >= 400 => throw new Exception("Error encountered while fetching responses")
			case _ => new File(fileName)
		}
	}
}