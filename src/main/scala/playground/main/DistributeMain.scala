package playground.main

import org.apache.spark.sql.SparkSession
import playground.jobs.etl.DistributeSJob
import playground.utils.SparkUtils

object DistributeMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("distributeMain")

    val distribute = new DistributeSJob()
    distribute.execute()
  }
}
