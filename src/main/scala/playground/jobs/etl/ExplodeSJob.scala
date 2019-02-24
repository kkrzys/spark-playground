package playground.jobs.etl

import com.databricks.spark.avro._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import playground.jobs.SJob

class ExplodeSJob(implicit sparkSession: SparkSession) extends SJob {

  import sparkSession.implicits._

  override def execute(): Unit = {
    val footballMatchCompleteDf = sparkSession.read.avro("data/raw/FootballMatchCompleted")
    val eplStandingReceiveDf = sparkSession.read.avro("data/raw/EplStandingReceived")

    val transformedFootballMatchCompleteDf = footballMatchCompleteDf.select(
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

    val res1 = homeTeamDf
      .join(awayTeamDf, Seq("team", "match_year_date"))
      .withColumn("total_goals", 'total_full_time_home_goals + 'total_full_time_away_goals)
      .drop("total_full_time_home_goals", "total_full_time_away_goals")

    val res2 = eplStandingReceiveDf
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

    val result = res1.join(res2, Seq("team", "match_year_date"))

    result.show(50)

    val teamStatisticsHiveTablePath = "data/prepare/TeamStatistics"

    result.coalesce(sparkSession.sparkContext.defaultParallelism)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(teamStatisticsHiveTablePath)
  }
}
