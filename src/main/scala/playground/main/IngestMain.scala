package playground.main

import org.apache.spark.sql.SparkSession
import playground.jobs.etl.IngestSJob
import playground.utils.SparkUtils

object IngestMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("ingestMain")

    val ingestSJob = new IngestSJob()

    ingestSJob.execute()
  }
}
