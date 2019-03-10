package playground.api

import org.apache.spark.sql.SparkSession
import playground.api.Container.DatasetName

import scala.language.higherKinds

private[api] trait Container[F[_], A] {
  protected def inputDataset(implicit sparkSession: SparkSession): F[A]

  protected def mapDataset(inputDataset: F[A])(implicit sparkSession: SparkSession): Map[DatasetName, F[_]]

  protected def combineDatasets(inputDatasets: Map[DatasetName, F[_]])
                               (implicit sparkSession: SparkSession): Option[Map[DatasetName, F[_]]] = None
}

object Container {

  trait DatasetName

  def run[F[_], A](c: Container[F, A])(implicit sparkSession: SparkSession): Map[DatasetName, F[_]] = {
    val mapped = c.mapDataset(c.inputDataset)
    c.combineDatasets(mapped).getOrElse(mapped)
  }

  def toContainer[F[_], A](inputDS: F[A],
                           mapDatasetFunc: F[A] => Map[DatasetName, F[_]],
                           combineDatasetsFunc: Map[DatasetName, F[_]] => Option[Map[DatasetName, F[_]]]): Container[F, A] = {
    new Container[F, A] {
      override protected def inputDataset(implicit sparkSession: SparkSession): F[A] = inputDS

      override protected def mapDataset(inputDataset: F[A])
                                       (implicit sparkSession: SparkSession): Map[DatasetName, F[_]] =
        mapDatasetFunc(inputDataset)

      override protected def combineDatasets(inputDatasets: Map[DatasetName, F[_]])(implicit sparkSession: SparkSession): Option[Map[DatasetName, F[_]]] =
        combineDatasetsFunc(inputDatasets)
    }
  }
}
