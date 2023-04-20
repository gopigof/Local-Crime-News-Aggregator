package edu.neu.coe.csye7200.crimeagg.workflows

case class Workflow(
					   source: Source,
					   validations: List[String],
					   transformations: List[String],
					   schemaValidations: List[String],
					   sink: Sink
				   )

case class Source (`type`: String, path: String, metaConfig: Map[String, String])

case class Sink (`type`: String, metaConfig: Map[String, String])
