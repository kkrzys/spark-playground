package playground.main.examples

import org.apache.spark.sql.SparkSession
import playground.jobs.examples.UpdateRecordInDfExample
import playground.utils.SparkUtils

object UpdateRecordInDfMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("updateRecordInDfMain")

    val updateRecordInDfJob = new UpdateRecordInDfExample()
    updateRecordInDfJob.execute()
  }
}
