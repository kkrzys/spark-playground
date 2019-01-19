package playground.jobs.etl

import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession
import playground.jobs.SJob
import playground.transformations.FootballMatchCompletedDistributeTransformations

class DistributeSJob(footballMatchCompletedTransformations: FootballMatchCompletedDistributeTransformations)
                    (implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {
    val footballMatchCompleteDf = sparkSession.read.avro("data/raw/ingestion/FootballMatchCompleted")
    val eplStandingReceiveDf = sparkSession.read.avro("data/raw/ingestion/EplStandingReceived")

    val resultFootballMatchCompleteDf = footballMatchCompletedTransformations.apply(footballMatchCompleteDf)
    //TODO

    eplStandingReceiveDf.printSchema()
    eplStandingReceiveDf.show()

    val footballMatchCompletedPath = "data/raw/FootballMatchCompleted"
    val eplStandingReceivedPath = "data/raw/EplStandingReceived"

    //resultFootballMatchCompleteDf.write.mode(SaveMode.Overwrite).partitionBy("match_year_date")
      //.avro(footballMatchCompletedPath)
  }
}
