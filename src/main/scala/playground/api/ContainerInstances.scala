package playground.api

import com.databricks.spark.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import playground.api.Container.DatasetName

object ContainerInstances {

  object Explode {

    lazy val footballMatchCompleteContainer = new Container[Dataset, Row] {
      import DatasetNames.Explode.FootballMatchCompleteDatasetNames._

      override protected def inputDataset(implicit sparkSession: SparkSession): DataFrame =
        sparkSession.read.avro("data/raw/FootballMatchCompleted")

      override protected def mapDataset(inputDataset: DataFrame)(implicit sparkSession: SparkSession): Map[DatasetName, Dataset[_]] = {
        import sparkSession.implicits._

        val transformedFootballMatchCompleteDf = inputDataset.select(
          'HomeTeam.as("home_team"),
          'AwayTeam.as("away_team"),
          'FTHG.cast(IntegerType).as("full_time_home_goals"),
          'FTAG.cast(IntegerType).as("full_time_away_goals"),
          'match_year_date
        ).cache()

        val homeTeamDf = transformedFootballMatchCompleteDf
          .withColumnRenamed("home_team", "team")
          .groupBy('team, 'match_year_date)
          .agg(sum("full_time_home_goals").alias("total_full_time_home_goals"))

        val awayTeamDf = transformedFootballMatchCompleteDf
          .withColumnRenamed("away_team", "team")
          .groupBy('team, 'match_year_date)
          .agg(sum("full_time_away_goals").alias("total_full_time_away_goals"))

        Map(HomeDf -> homeTeamDf, AwayDf -> awayTeamDf)
      }

      override protected def combineDatasets(inputDatasets: Map[DatasetName, Dataset[_]])
                                  (implicit sparkSession: SparkSession): Option[Map[DatasetName, Dataset[_]]] = {
        import sparkSession.implicits._

        Some(Map(Res1 ->
          inputDatasets(HomeDf)
            .join(inputDatasets(AwayDf), Seq("team", "match_year_date"))
            .withColumn("total_goals", 'total_full_time_home_goals + 'total_full_time_away_goals)
            .drop("total_full_time_home_goals", "total_full_time_away_goals")
        ))
      }
    }

    lazy val eplStandingReceiveContainer = new Container[Dataset, Row] {
      import DatasetNames.Explode.EplStandingReceiveDatasetNames._

      override protected def inputDataset(implicit sparkSession: SparkSession): DataFrame =
        sparkSession.read.avro("data/raw/EplStandingReceived")

      override protected def mapDataset(inputDataset: DataFrame)
                             (implicit sparkSession: SparkSession): Map[DatasetName, Dataset[_]] = {
        import sparkSession.implicits._

        val res2 = inputDataset
          .withColumn("years",
            array(struct('y2000.as("y1"), lit(2000).as("y2")),
              struct('y2001.as("y1"), lit(2001).as("y2")),
              struct('y2002.as("y1"), lit(2002).as("y2")),
              struct('y2003.as("y1"), lit(2003).as("y2")),
              struct('y2004.as("y1"), lit(2004).as("y2")),
              struct('y2005.as("y1"), lit(2005).as("y2")),
              struct('y2006.as("y1"), lit(2006).as("y2")),
              struct('y2007.as("y1"), lit(2007).as("y2")),
              struct('y2008.as("y1"), lit(2008).as("y2")),
              struct('y2009.as("y1"), lit(2009).as("y2")),
              struct('y2010.as("y1"), lit(2010).as("y2")),
              struct('y2011.as("y1"), lit(2011).as("y2")),
              struct('y2012.as("y1"), lit(2012).as("y2")),
              struct('y2013.as("y1"), lit(2013).as("y2")),
              struct('y2014.as("y1"), lit(2014).as("y2")),
              struct('y2015.as("y1"), lit(2015).as("y2")),
              struct('y2016.as("y1"), lit(2016).as("y2"))))
          .withColumn("epl_standing_with_year", explode('years))
          .withColumn("epl_standing", 'epl_standing_with_year.getField("y1"))
          .withColumn("match_year_date", 'epl_standing_with_year.getField("y2"))
          .withColumnRenamed("Team", "team")
          .drop("y2000", "y2001", "y2002", "y2003", "y2004",
            "y2005", "y2006", "y2007", "y2008", "y2009", "y2010",
            "y2011", "y2012", "y2013", "y2014", "y2015", "y2016", "years", "epl_standing_with_year")

        Map(Res2 -> res2)
      }
    }
  }

}
