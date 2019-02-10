package playground.jobs.etl

import com.databricks.spark.avro._
import org.apache.spark.sql.{SaveMode, SparkSession}
import playground.jobs.SJob
import playground.transformations.{EplStandingReceivedDistributeTransformations, FootballMatchCompletedDistributeTransformations}

class DistributeSJob(footballMatchCompletedTransformations: FootballMatchCompletedDistributeTransformations,
                     eplStandingReceivedDistributeTransformations: EplStandingReceivedDistributeTransformations)
                    (implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {
    val footballMatchCompleteDf = sparkSession.read.avro("data/raw/ingestion/FootballMatchCompleted")
    val eplStandingReceiveDf = sparkSession.read.avro("data/raw/ingestion/EplStandingReceived")

    val resultFootballMatchCompleteDf = footballMatchCompletedTransformations.apply(footballMatchCompleteDf)
    val resultEplStandingReceiveDf = eplStandingReceivedDistributeTransformations.apply(eplStandingReceiveDf)

    val footballMatchCompletedPath = "data/raw/FootballMatchCompleted"
    val eplStandingReceivedPath = "data/raw/EplStandingReceived"

    resultFootballMatchCompleteDf.write.mode(SaveMode.Overwrite).partitionBy("match_year_date")
      .avro(footballMatchCompletedPath)

    resultEplStandingReceiveDf.write.mode(SaveMode.Overwrite).avro(eplStandingReceivedPath)
  }
}
