package playground.jobs.etl

import org.apache.spark.sql.{SaveMode, SparkSession}
import playground.core.DatasetNames.Explode.EplStandingReceiveDatasetNames._
import playground.core.DatasetNames.Explode.FootballMatchCompleteDatasetNames._
import playground.core.{DataContainerInstances, DataContainerUtils}
import playground.jobs.SJob

class ExplodeSJob(implicit sparkSession: SparkSession) extends SJob {

  override def execute(): Unit = {

    import DataContainerInstances.Explode._

    val result =
      DataContainerUtils
        .join(footballMatchCompleteContainer, eplStandingReceiveContainer)(Res1, Res2)(Seq("team", "match_year_date"))

    result.show(50)

    val teamStatisticsHiveTablePath = "data/prepare/TeamStatistics"

    result.coalesce(sparkSession.sparkContext.defaultParallelism)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(teamStatisticsHiveTablePath)
  }
}
