package playground.jobs.etl

import org.apache.spark.sql.{SaveMode, SparkSession}
import playground.api.DatasetNames.Explode.EplStandingReceiveDatasetNames._
import playground.api.DatasetNames.Explode.FootballMatchCompleteDatasetNames._
import playground.api.{ContainerInstances, ContainerUtils}
import playground.jobs.SJob

class ExplodeSJob(implicit sparkSession: SparkSession) extends SJob {

  override def execute(): Unit = {

    import ContainerInstances.Explode._

    val result =
      ContainerUtils
        .join(footballMatchCompleteContainer, eplStandingReceiveContainer)(Res1, Res2)(Seq("team", "match_year_date"))

    result.show(50)

    val teamStatisticsHiveTablePath = "data/prepare/TeamStatistics"

    result.coalesce(sparkSession.sparkContext.defaultParallelism)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(teamStatisticsHiveTablePath)
  }
}
