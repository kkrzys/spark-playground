package playground.jobs.etl

import org.apache.spark.sql.{SaveMode, SparkSession}
import playground.api.EplStandingReceiveExplodeDatasetNames._
import playground.api.FootballMatchCompleteExplodeDatasetNames._
import playground.api.{ContainerInstances, ContainerUtils}
import playground.jobs.SJob

class ExplodeSJob(implicit sparkSession: SparkSession) extends SJob {

  override def execute(): Unit = {

    val explodeContainerInstances = ContainerInstances.Explode

    val footballMatchCompleteContainer = explodeContainerInstances.footballMatchCompleteContainer
    val eplStandingReceiveContainer = explodeContainerInstances.eplStandingReceiveContainer

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
