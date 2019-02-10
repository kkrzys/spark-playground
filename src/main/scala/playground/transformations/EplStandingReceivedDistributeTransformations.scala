package playground.transformations

import org.apache.spark.sql.DataFrame

class EplStandingReceivedDistributeTransformations extends Transformations {
  override def transformations: Seq[Transformation] = Seq(
    (df: DataFrame) => df.drop("batch_id")
  )
}
