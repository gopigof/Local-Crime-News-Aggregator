package edu.neu.coe.csye7200.models

trait Status {
	val status: String
}

case object StatusOk extends Status {
	override val status: String = "OK"
}

case object StatusError extends Status {
	override val status: String = "ERROR"
}