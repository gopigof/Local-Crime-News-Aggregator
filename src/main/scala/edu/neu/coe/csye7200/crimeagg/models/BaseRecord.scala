package edu.neu.coe.csye7200.crimeagg.models

case class BaseRecord(data: Map[String, Any], status: Status = StatusOk, reason: Option[String] = None)

