package playground.main

import org.apache.spark.sql.SparkSession
import playground.jobs.etl.ExplodeSJob
import playground.utils.SparkUtils

object ExplodeMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("prepareMain")

    val prepare = new ExplodeSJob()
    prepare.execute()
  }
}
