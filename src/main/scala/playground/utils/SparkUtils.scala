package playground.utils

import java.io.File

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(appName: String): SparkSession = {
    sparkSessionCommonBuilder(appName)
      .getOrCreate()
  }

  def createHiveSparkSession(appName: String): SparkSession = {
    val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

    sparkSessionCommonBuilder(appName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
  }

  private def sparkSessionCommonBuilder(appName: String): SparkSession.Builder =
    SparkSession.builder()
      .master("local[*]")
      .appName(appName)
}
