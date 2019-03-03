package playground.api

import org.apache.spark.sql.SparkSession
import playground.api.Container.DatasetName

import scala.language.higherKinds

private[api] trait Container[F[_], A] {
  def inputDataset(implicit sparkSession: SparkSession): F[A]

  def mapDataset(inputDataset: F[A])(implicit sparkSession: SparkSession): Map[DatasetName, F[_]]

  def combineDatasets(inputDatasets: Map[DatasetName, F[_]])
                     (implicit sparkSession: SparkSession): Option[Map[DatasetName, F[_]]] = None
}

object Container {

  trait DatasetName

  def run[F[_], A](c: Container[F, A])(implicit sparkSession: SparkSession): Map[DatasetName, F[_]] = {
    val mapped = c.mapDataset(c.inputDataset)
    c.combineDatasets(mapped).getOrElse(mapped)
  }
}
