package playground.transformations

import org.apache.spark.sql.DataFrame

trait Transformations {
  type Transformation = DataFrame => DataFrame

  def transformations: Seq[Transformation]

  final def apply(inputDataFrame: DataFrame): DataFrame =
    transformations.foldLeft(inputDataFrame)((el, f) => f(el))
}
