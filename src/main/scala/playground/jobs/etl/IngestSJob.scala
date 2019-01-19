package playground.jobs.etl

import com.databricks.spark.avro._
import org.apache.spark.sql.{SaveMode, SparkSession}
import playground.jobs.SJob
import playground.transformations.{EplStandingReceivedIngestTransformations, FootballMatchCompletedIngestTransformations}

class IngestSJob(footballMatchCompletedTransformations: FootballMatchCompletedIngestTransformations,
                 eplStandingReceivedTransformations: EplStandingReceivedIngestTransformations)
                (implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {
    val footballMatchCompleteDf = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/data/landing/topics/dev-footballmatch-complete/*.csv")
    val eplStandingReceiveDf = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/data/landing/topics/dev-eplstanding-receive/*.csv")

    val resultFootballMatchCompleteDf = footballMatchCompletedTransformations.apply(footballMatchCompleteDf)
    val resultEplStandingReceiveDf = eplStandingReceivedTransformations.apply(eplStandingReceiveDf)

    val footballMatchCompletedPath = "data/raw/ingestion/FootballMatchCompleted"
    val eplStandingReceivedPath = "data/raw/ingestion/EplStandingReceived"

    resultFootballMatchCompleteDf.write.mode(SaveMode.Overwrite).partitionBy("batch_id")
      .avro(footballMatchCompletedPath)
    resultEplStandingReceiveDf.write.mode(SaveMode.Overwrite).partitionBy("batch_id")
      .avro(eplStandingReceivedPath)
  }
}
