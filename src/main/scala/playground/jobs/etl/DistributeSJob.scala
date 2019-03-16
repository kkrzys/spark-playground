package playground.jobs.etl

import com.databricks.spark.avro._
import org.apache.spark.sql.{SaveMode, SparkSession}
import playground.api.Container
import playground.jobs.SJob

class DistributeSJob(implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {

    import playground.api.ContainerInstances.Distribute._
    import playground.api.DatasetNames.Distribute._

    val footballMatchCompleteC = Container.run(footballMatchCompleteContainer)
    val eplStandingReceiveC = Container.run(eplStandingReceiveContainer)

    val footballMatchCompletedPath = "data/raw/FootballMatchCompleted"
    val eplStandingReceivedPath = "data/raw/EplStandingReceived"

    footballMatchCompleteC(FootballMatchCompleteDatasetNames.ResultFootballMatchCompleteDf)
      .write.mode(SaveMode.Overwrite).partitionBy("match_year_date")
      .avro(footballMatchCompletedPath)

    eplStandingReceiveC(EplStandingReceiveDatasetNames.ResultEplStandingReceiveDf)
      .write.mode(SaveMode.Overwrite).avro(eplStandingReceivedPath)
  }
}
