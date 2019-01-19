package playground.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import playground.utils.RandomGenerator

class FootballMatchCompletedIngestTransformations extends Transformations {

  val (rangeStart, rangeEnd) = (2000000, 3000000)

  val Batches: Seq[Int] = Seq(RandomGenerator.between(rangeStart, rangeEnd),
    RandomGenerator.between(rangeStart, rangeEnd))

  override def transformations: Seq[Transformation] = Seq(
    (df: DataFrame) => df.withColumnRenamed("BbMx>2.5", "BbMxGreaterThan2Point5")
      .withColumnRenamed("BbAv>2.5", "BbAvGreaterThan2Point5")
      .withColumnRenamed("BbMx<2.5", "BbMxLessThan2Point5")
      .withColumnRenamed("BbAv<2.5", "BbAvLessThan2Point5"),
    (df: DataFrame) =>
      df.withColumn("batch_id", when(col("BbOU") > 30, Batches.head).otherwise(Batches(1)))
  )
}
