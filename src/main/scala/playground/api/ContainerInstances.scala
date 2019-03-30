package playground.api

import com.databricks.spark.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql._
import playground.api.Container.DatasetName
import playground.utils.RandomGenerator

object ContainerInstances {

  object Ingest {
    lazy val footbalMatchCompleteContainer = new Container[Dataset, Row] {

      import DatasetNames.Ingest.FootballMatchCompleteDatasetNames._

      override protected def inputDataset(implicit sparkSession: SparkSession): DataFrame =
        sparkSession.read.option("header", "true")
          .csv("src/main/resources/data/landing/topics/dev-footballmatch-complete/*.csv")

      override protected def mapDataset(inputDataset: DataFrame)
                                       (implicit sparkSession: SparkSession): Map[DatasetName, Dataset[_]] = {
        val (rangeStart, rangeEnd) = (2000000, 3000000)

        val Batches: Seq[Int] = Seq(RandomGenerator.between(rangeStart, rangeEnd),
          RandomGenerator.between(rangeStart, rangeEnd))

        val res = inputDataset.withColumnRenamed("BbMx>2.5", "BbMxGreaterThan2Point5")
          .withColumnRenamed("BbAv>2.5", "BbAvGreaterThan2Point5")
          .withColumnRenamed("BbMx<2.5", "BbMxLessThan2Point5")
          .withColumnRenamed("BbAv<2.5", "BbAvLessThan2Point5")
          .withColumn("batch_id", when(col("BbOU") > 30, Batches.head).otherwise(Batches(1)))

        Map(ResultFootballMatchCompleteDf -> res)
      }

      override protected def writePath: String = "data/raw/ingestion/FootballMatchCompleted"

      override protected def writeFunc(datasets: Map[DatasetName, Dataset[_]], writePath: String): Unit =
        datasets(ResultFootballMatchCompleteDf)
          .write.mode(SaveMode.Overwrite).partitionBy("batch_id")
          .avro(writePath)
    }

    lazy val eplStandingReceiveContainer = new Container[Dataset, Row] {

      import DatasetNames.Ingest.EplStandingReceiveDatasetNames._

      override protected def inputDataset(implicit sparkSession: SparkSession): Dataset[Row] =
        sparkSession.read.option("header", "true")
          .csv("src/main/resources/data/landing/topics/dev-eplstanding-receive/*.csv")

      override protected def mapDataset(inputDataset: Dataset[Row])
                                       (implicit sparkSession: SparkSession): Map[DatasetName, Dataset[_]] = {
        val (rangeStart, rangeEnd) = (2000000, 3000000)
        val Batches: Seq[Int] = Seq(RandomGenerator.between(rangeStart, rangeEnd),
          RandomGenerator.between(rangeStart, rangeEnd))

        val YearColumns = (2000 to 2016).map(name => col(name.toString).as(s"y$name"))

        val res = inputDataset
          .withColumn("batch_id", when(col("2006").isNotNull, Batches.head).otherwise(Batches(1)))
          .select(Seq(col("Team"), col("batch_id")) ++ YearColumns: _*)

        Map(ResultEplStandingReceiveDf -> res)
      }

      override protected def writePath: String = "data/raw/ingestion/EplStandingReceived"

      override protected def writeFunc(datasets: Map[DatasetName, Dataset[_]], writePath: String): Unit =
        datasets(ResultEplStandingReceiveDf)
          .write.mode(SaveMode.Overwrite).partitionBy("batch_id")
          .avro(writePath)
    }
  }

  object Distribute {
    lazy val footballMatchCompleteContainer = new Container[Dataset, Row] {

      import DatasetNames.Distribute.FootballMatchCompleteDatasetNames._

      override protected def inputDataset(implicit sparkSession: SparkSession): DataFrame =
        sparkSession.read.avro("data/raw/ingestion/FootballMatchCompleted")

      override protected def mapDataset(inputDataset: DataFrame)
                                       (implicit sparkSession: SparkSession): Map[DatasetName, Dataset[_]] = {
        val res = inputDataset.drop("batch_id")
          .withColumn("match_year_date", date_format(to_date(col("Date"), "dd/MM/yy"), "yyyy"))

        Map(ResultFootballMatchCompleteDf -> res)
      }

      override protected def writePath: String = "data/raw/FootballMatchCompleted"

      override protected def writeFunc(datasets: Map[DatasetName, Dataset[_]], writePath: String): Unit =
        datasets(ResultFootballMatchCompleteDf)
          .write.mode(SaveMode.Overwrite).partitionBy("match_year_date")
          .avro(writePath)
    }

    lazy val eplStandingReceiveContainer = new Container[Dataset, Row] {

      import DatasetNames.Distribute.EplStandingReceiveDatasetNames._

      override protected def inputDataset(implicit sparkSession: SparkSession): DataFrame =
        sparkSession.read.avro("data/raw/ingestion/EplStandingReceived")

      override protected def mapDataset(inputDataset: DataFrame)
                                       (implicit sparkSession: SparkSession): Map[DatasetName, Dataset[_]] = {
        val res = inputDataset.drop("batch_id")

        Map(ResultEplStandingReceiveDf -> res)
      }

      override protected def writePath: String = "data/raw/EplStandingReceived"

      override protected def writeFunc(datasets: Map[DatasetName, Dataset[_]], writePath: String): Unit =
        datasets(ResultEplStandingReceiveDf)
          .write.mode(SaveMode.Overwrite).avro(writePath)
    }
  }

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
