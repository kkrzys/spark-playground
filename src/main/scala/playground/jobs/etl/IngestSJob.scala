package playground.jobs.etl

import com.databricks.spark.avro._
import org.apache.spark.sql.{SaveMode, SparkSession}
import playground.api.Container
import playground.jobs.SJob

class IngestSJob(implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {

    import playground.api.ContainerInstances.Ingest._
    import playground.api.DatasetNames.Ingest._

    val footballMatchCompleteC = Container.run(footbalMatchCompleteContainer)
    val eplStandingReceiveC = Container.run(eplStandingReceiveContainer)

    val footballMatchCompletedPath = "data/raw/ingestion/FootballMatchCompleted"
    val eplStandingReceivedPath = "data/raw/ingestion/EplStandingReceived"

    footballMatchCompleteC(FootballMatchCompleteDatasetNames.ResultFootballMatchCompleteDf)
      .write.mode(SaveMode.Overwrite).partitionBy("batch_id")
      .avro(footballMatchCompletedPath)
    eplStandingReceiveC(EplStandingReceiveDatasetNames.ResultEplStandingReceiveDf)
      .write.mode(SaveMode.Overwrite).partitionBy("batch_id")
      .avro(eplStandingReceivedPath)
  }
}
