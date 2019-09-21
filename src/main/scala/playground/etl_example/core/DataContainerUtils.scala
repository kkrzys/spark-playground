package playground.etl_example.core

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import playground.etl_example.core.DataContainer.DatasetName

object DataContainerUtils {
  def join[A, B](c1: DataContainer[Dataset, A], c2: DataContainer[Dataset, B])
                (d1: DatasetName, d2: DatasetName)
                (joinFields: Seq[String])
                (implicit sparkSession: SparkSession): DataFrame = {
    val res1: Map[DatasetName, Dataset[_]] =
      DataContainer.run(c1)
    val res2: Map[DatasetName, Dataset[_]] =
      DataContainer.run(c2)

    res1(d1).join(res2(d2), joinFields)
  }
}
