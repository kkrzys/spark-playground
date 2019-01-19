package playground.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import playground.utils.RandomGenerator

class EplStandingReceivedIngestTransformations extends Transformations {

  val (rangeStart, rangeEnd) = (2000000, 3000000)
  val Batches: Seq[Int] = Seq(RandomGenerator.between(rangeStart, rangeEnd),
    RandomGenerator.between(rangeStart, rangeEnd))

  val YearColumns = (2000 to 2016).map(name => col(name.toString).as(s"y$name"))

  override def transformations: Seq[Transformation] = Seq(
    (df: DataFrame) =>
      df.withColumn("batch_id", when(col("2006").isNotNull, Batches.head).otherwise(Batches(1))),
    (df: DataFrame) => df.select(Seq(col("Team"), col("batch_id")) ++ YearColumns: _*)
  )
}
