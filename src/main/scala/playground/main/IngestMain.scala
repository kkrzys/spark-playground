package playground.main

import org.apache.spark.sql.SparkSession
import playground.jobs.etl.IngestSJob
import playground.transformations.{EplStandingReceivedIngestTransformations, FootballMatchCompletedIngestTransformations}
import playground.utils.SparkUtils

object IngestMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("ingestMain")

    val footballMatchCompletedTransformations = new FootballMatchCompletedIngestTransformations()
    val eplStandingResultTransformations = new EplStandingReceivedIngestTransformations()

    val ingestSJob =
      new IngestSJob(footballMatchCompletedTransformations, eplStandingResultTransformations)

    ingestSJob.execute()
  }
}
