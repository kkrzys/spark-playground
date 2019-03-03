package playground.jobs.etl

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import playground.api.EplStandingReceiveExplodeDatasetNames._
import playground.api.FootballMatchCompleteExplodeDatasetNames._
import playground.api.{Container, ContainerInstances}
import playground.jobs.SJob

class ExplodeSJob(implicit sparkSession: SparkSession) extends SJob {

  override def execute(): Unit = {

    val explodeContainerInstances = ContainerInstances.Explode

    val footballMatchCompleteContainer = explodeContainerInstances.footballMatchCompleteContainer
    val eplStandingReceiveContainer = explodeContainerInstances.eplStandingReceiveContainer

    val footballMatchCompleteContainerResult: Map[Container.DatasetName, Dataset[_]] =
      Container.run(footballMatchCompleteContainer)
    val eplStandingReceiveContainerResult: Map[Container.DatasetName, Dataset[_]] =
      Container.run(eplStandingReceiveContainer)

    val result = footballMatchCompleteContainerResult(Res1)
      .join(eplStandingReceiveContainerResult(Res2), Seq("team", "match_year_date"))

    result.show(50)

    val teamStatisticsHiveTablePath = "data/prepare/TeamStatistics"

    result.coalesce(sparkSession.sparkContext.defaultParallelism)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(teamStatisticsHiveTablePath)
  }
}
