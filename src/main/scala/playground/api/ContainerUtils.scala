package playground.api

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import playground.api.Container.DatasetName

object ContainerUtils {
  def join[A, B](c1: Container[Dataset, A], c2: Container[Dataset, B])
                (d1: DatasetName, d2: DatasetName)
                (joinFields: Seq[String])
                (implicit sparkSession: SparkSession): DataFrame = {
    val res1: Map[DatasetName, Dataset[_]] =
      Container.run(c1)
    val res2: Map[DatasetName, Dataset[_]] =
      Container.run(c2)

    res1(d1).join(res2(d2), joinFields)
  }
}
