package playground.jobs.examples

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import playground.jobs.SJob
import playground.utils.DataFrameUtils

class UpdateRecordInDfExample(implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {
    import sparkSession.implicits._

    //original dataframe
    val eplStandingReceiveDf = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/data/landing/topics/dev-eplstanding-receive/*.csv")

    //save to hive table initial data
    val hiveTableName = "updateRecordInDfExample"
    eplStandingReceiveDf.write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName)

    //read hive table with initial data
    val eplStandingHiveTableDf = sparkSession.read.table(hiveTableName)

    //insert new row to dataframe
    val newRowDf =
      Seq(("Monaco", 2, 3, null, 1, null, null, null, null, null, 1,
        null, null, null, null, null, null, null)).toDF()

    val eplStandingReceiveWithInsertedRowDf = eplStandingHiveTableDf.union(newRowDf)

    //find rows to update
    val rowsToUpdate =
      eplStandingReceiveWithInsertedRowDf
        .filter($"Team" === "Monaco" or $"Team" === "Wimbledon")

    rowsToUpdate.printSchema()
    rowsToUpdate.show()

    //update rows with missing values
    val rowsWithMissingValuesDf =
      Seq(("Monaco", null: Integer, null: Integer, 10, null: Integer, 11, 12, 13, 14, 15, null: Integer, 16, 17, 18, 19, 20, 21, 22),
        ("Wimbledon", null: Integer, Integer.valueOf(1), 2, Integer.valueOf(3), 4, 5, 6, 7, 8, Integer.valueOf(9), 10, 11, 12, 13, 14, 15, 16)).toDF()

    val updatedRowsWithMissingValues =
      rowsToUpdate.union(rowsWithMissingValuesDf)

    val mergedRows = updatedRowsWithMissingValues
      .groupBy("Team")
      .agg(
        first("2000", ignoreNulls = true).as("2000"),
        first("2001", ignoreNulls = true).as("2001"),
        first("2002", ignoreNulls = true).as("2002"),
        first("2003", ignoreNulls = true).as("2003"),
        first("2004", ignoreNulls = true).as("2004"),
        first("2005", ignoreNulls = true).as("2005"),
        first("2006", ignoreNulls = true).as("2006"),
        first("2007", ignoreNulls = true).as("2007"),
        first("2008", ignoreNulls = true).as("2008"),
        first("2009", ignoreNulls = true).as("2009"),
        first("2010", ignoreNulls = true).as("2010"),
        first("2011", ignoreNulls = true).as("2011"),
        first("2012", ignoreNulls = true).as("2012"),
        first("2013", ignoreNulls = true).as("2013"),
        first("2014", ignoreNulls = true).as("2014"),
        first("2015", ignoreNulls = true).as("2015"),
        first("2016", ignoreNulls = true).as("2016")
      )
    //--------------------------------------------
    mergedRows.printSchema()
    mergedRows.show(50)

    mergedRows.write.mode(SaveMode.Append).saveAsTable(hiveTableName)

    println("ALL RESULTS: ")
    sparkSession.read.table(hiveTableName).show(50)
  }

  def execute2(): Unit = {
    import sparkSession.implicits._

    //original dataframe
    val eplStandingReceiveDf = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/data/landing/topics/dev-eplstanding-receive/*.csv")

    val hiveTableName = "updateRecordInDfExample"

    val rowDf =
      Seq(("Monaco", 2, 3, null, 1, null, null, null, null, null, 1,
        null, null, null, null, null, null, null)).toDF()

    DataFrameUtils.updateDf(eplStandingReceiveDf, rowDf, "Team")(hiveTableName)

    val eplStandingHiveTableDf = sparkSession.read.table(hiveTableName)

    val newRowsDf =
      Seq(("Monaco", null: Integer, null: Integer, 10, null: Integer, 11, 12, 13, 14, 15, null: Integer, 16, 17, 18, 19, 20, 21, 22),
        ("NonExisting", null: Integer, null: Integer, 100, null: Integer, 111, 122, 133, 144, 155, null: Integer, 166, 177, 188, 199, 200, 211, 222),
        ("Wimbledon", null: Integer, Integer.valueOf(1), 2, Integer.valueOf(3), 4, 5, 6, 7, 8, Integer.valueOf(9), 10, 11, 12, 13, 14, 15, 16)).toDF()

    DataFrameUtils.updateDf(eplStandingHiveTableDf, newRowsDf, "Team")(hiveTableName)

    sparkSession.read.table(hiveTableName).show(50)
  }
}
