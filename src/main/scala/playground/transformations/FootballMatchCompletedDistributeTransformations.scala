package playground.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class FootballMatchCompletedDistributeTransformations extends Transformations {
  override def transformations: Seq[Transformation] = Seq(
    (df: DataFrame) => df.drop("batch_id"),
    (df: DataFrame) =>
      df.withColumn("match_year_date", date_format(to_date(col("Date"), "dd/MM/yy"), "yyyy"))
  )
}
