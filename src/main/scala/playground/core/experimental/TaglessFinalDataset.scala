package playground.core.experimental

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import playground.core.experimental.DataContainerAlgebra.IngestInterpreter
import playground.core.experimental.DatasetNames.DatasetName
import playground.core.experimental.DatasetNames.Ingest.FootballMatchCompleteDatasetNames.ResultFootballMatchCompleteDf
import playground.utils.{RandomGenerator, SparkUtils}

import scala.language.higherKinds

trait DataContainerAlgebra[DataContainer[_], T] {
  def inputDataset: DataContainer[T]

  def mapDataset(inputDataset: DataContainer[T]): Map[DatasetName, DataContainer[_]]

  def combineDatasets(inputDatasets: Map[DatasetName, DataContainer[_]]): Option[Map[DatasetName, DataContainer[_]]] = None

  def writePath: String =
    throw new NotImplementedError(s"writePath is not defined!")

  def writeFunc(datasets: Map[DatasetName, DataContainer[_]], writePath: String): Unit =
    throw new NotImplementedError("writeFunc is not implemented!")
}

object DataContainerAlgebra {

  class IngestInterpreter(implicit spark: SparkSession) extends DataContainerAlgebra[Dataset, Row] {
    override def inputDataset: Dataset[Row] = spark.read.option("header", "true")
      .csv("src/main/resources/data/landing/topics/dev-footballmatch-complete/*.csv")

    override def mapDataset(inputDataset: Dataset[Row]): Map[DatasetName, Dataset[_]] = {
      val (rangeStart, rangeEnd) = (2000000, 3000000)

      val Batches: Seq[Int] = Seq(RandomGenerator.between(rangeStart, rangeEnd),
        RandomGenerator.between(rangeStart, rangeEnd))

      val res = inputDataset.withColumnRenamed("BbMx>2.5", "BbMxGreaterThan2Point5")
        .withColumnRenamed("BbAv>2.5", "BbAvGreaterThan2Point5")
        .withColumnRenamed("BbMx<2.5", "BbMxLessThan2Point5")
        .withColumnRenamed("BbAv<2.5", "BbAvLessThan2Point5")
        .withColumn("batch_id", when(col("BbOU") > 30, Batches.head).otherwise(Batches(1)))

      Map(ResultFootballMatchCompleteDf -> res)
    }

    override def writePath: String = "data/raw/ingestion/FootballMatchCompleted"

    override def writeFunc(datasets: Map[DatasetName, Dataset[_]], writePath: String): Unit =
      datasets(ResultFootballMatchCompleteDf)
        .write.format("avro").mode(SaveMode.Overwrite).partitionBy("batch_id")
        .save(writePath)
  }

}

class DataContainerResolver[DataContainer[_], T](dc: DataContainerAlgebra[DataContainer, T]) {
  def resolve(write: Boolean = false): Map[DatasetName, DataContainer[_]] = {
    val mapped = dc.mapDataset(dc.inputDataset)
    val maybeCombined = dc.combineDatasets(mapped).getOrElse(mapped)
    //side effect
    if (write) dc.writeFunc(maybeCombined, dc.writePath)

    maybeCombined
  }
}

object DataContainerRunner extends App {
  implicit val sparkSession: SparkSession = SparkUtils.createSparkSession("TaglessFinal")

  new DataContainerResolver(new IngestInterpreter).resolve(write = true)
}
