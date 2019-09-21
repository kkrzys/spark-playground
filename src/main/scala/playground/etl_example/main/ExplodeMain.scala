package playground.etl_example.main

import org.apache.spark.sql.SparkSession
import playground.etl_example.jobs.etl.ExplodeSJob
import playground.utils.SparkUtils

object ExplodeMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("prepareMain")

    val prepare = new ExplodeSJob()
    prepare.execute()
  }
}
