package playground.delta_io_example

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import playground.utils.SparkUtils

object DeltaExampleApp extends App {
  implicit val spark = SparkUtils.createSparkSession("delta-example")

  val deltaTablePath = "delta_tables/eplstandings"

  val data = spark.read.option("header", "true")
    .csv("src/main/resources/data/landing/topics/dev-eplstanding-receive/*.csv")
  data.write.format("delta").mode("overwrite").save(deltaTablePath)

  //val updates = spark.range(10, 30).as("updates").toDF

  val deltaTable = DeltaTable.forPath(spark, deltaTablePath)

  deltaTable.updateExpr(
    "Team = 'Arsenal'",
    Map("2000" -> "'4'", "2001" -> "'4'"))

  deltaTable.toDF.show()
}

object DeltaExample {
  def vacuumCleaner(deltaTable: DeltaTable)(implicit spark: SparkSession): DataFrame = {
    spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")

    deltaTable.vacuum(0)
  }
}
