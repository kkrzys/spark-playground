package playground.main

import org.apache.spark.sql.SparkSession
import playground.jobs.etl.DistributeSJob
import playground.transformations.FootballMatchCompletedDistributeTransformations
import playground.utils.SparkUtils

object DistributeMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("distributeMain")

    val footballMatchCompletedTransformations = new FootballMatchCompletedDistributeTransformations()

    val distribute = new DistributeSJob(footballMatchCompletedTransformations)
    distribute.execute()
  }
}
