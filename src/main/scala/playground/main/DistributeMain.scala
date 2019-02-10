package playground.main

import org.apache.spark.sql.SparkSession
import playground.jobs.etl.DistributeSJob
import playground.transformations.{EplStandingReceivedDistributeTransformations, FootballMatchCompletedDistributeTransformations}
import playground.utils.SparkUtils

object DistributeMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("distributeMain")

    val footballMatchCompletedTransformations = new FootballMatchCompletedDistributeTransformations()
    val eplStandingReceivedTransformations = new EplStandingReceivedDistributeTransformations()

    val distribute = new DistributeSJob(footballMatchCompletedTransformations, eplStandingReceivedTransformations)
    distribute.execute()
  }
}
