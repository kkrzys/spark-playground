package playground.utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataFrameUtils {
  def updateDf(df: DataFrame, newRowsDf: DataFrame, idCol: String)(hiveTable: String)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    val d1 = newRowsDf.map(_.getAs[String]("_1"))
    val d2 = df.map(_.getAs[String](idCol))

    val intersect = d1.intersect(d2)

    val res = if (intersect.head(1).isEmpty)
      newRowsDf.toDF(df.columns: _*)
    else {
      val columnsWithoutIdKey = df.columns.filter(_ != idCol)
      val mergedFormula = columnsWithoutIdKey
        .map(colName => first(colName, ignoreNulls = true).as(colName))
        .toSeq

      val mergedRows = (df union newRowsDf)
        .groupBy(idCol)
        .agg(
          mergedFormula.head,
          mergedFormula.tail: _*
        )

      mergedRows
        .join(d1, mergedRows(idCol) === d1("value"), "right_outer")
        .drop("value")
    }
    //res.show(50)

    res.write.mode(SaveMode.Append).saveAsTable(hiveTable)
  }
}
