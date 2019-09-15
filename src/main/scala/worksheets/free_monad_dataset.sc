import cats.free.Free
import cats.free.Free.liftF
import cats.{Id, ~>}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import playground.utils.SparkUtils

sealed trait SOperationT[T]

case class DataSetTransformation[A, B](ds: Dataset[A], t: Dataset[A] => Dataset[B]) extends SOperationT[Dataset[B]]

type SOperation[T] = Free[SOperationT, T]

def transform[A, B](ds: Dataset[A], t: Dataset[A] => Dataset[B]): SOperation[Dataset[B]] =
  liftF[SOperationT, Dataset[B]](DataSetTransformation(ds, t))

//---------------------------------------------------------------------------

implicit val spark: SparkSession = SparkUtils.createSparkSession("spark")
implicit val sc: SparkContext = spark.sparkContext

import spark.implicits._

val df = Seq(("USA", 1, 1, "other"), ("USA", 11, 11, "other"), ("Poland", 2, 2, "other"),
  ("England", 3, 3, "other"), ("Ukraine", 44, 44, "other"),
  ("Ukraine", 4, 4, "other"), ("Ukraine", 444, 444, "other")).toDF("Country", "N1", "N2", "Other")

case class CountryInfo(name: String, n1: Int, n2: Int)
case class CountryInfo2(name: String, n1: Int)
case class CountryInfo3(countryName: String)

def program: SOperation[Dataset[Row]] =
  for {
    df1 <- transform(df, (el: DataFrame) => el.select("Country", "N1", "N2"))
    df2 <- transform(df1, (el: DataFrame) => el.withColumnRenamed("Country", "name"))
    ds1 <- transform(df2, (el: DataFrame) => el.as[CountryInfo])
    ds2 <- transform(ds1, (el: Dataset[CountryInfo]) => el.map(c => CountryInfo2(c.name, c.n1)))
    ds3 <- transform(ds2, (el: Dataset[CountryInfo2]) => el.map(c => CountryInfo3(c.name)))
    resDf <- transform(ds3, (el: Dataset[CountryInfo3]) => el.toDF())
  } yield resDf

//------------------------------------------------------------------------------

def dataSetCompiler: SOperationT ~> Id =
  new (SOperationT ~> Id) {
    override def apply[T](fa: SOperationT[T]): Id[T] = {
      fa match {
        case DataSetTransformation(dataSet, t) => t(dataSet).asInstanceOf[T]
      }
    }
  }

//---------------------------------------------------------------------------

val result: Dataset[Row] = program.foldMap(dataSetCompiler)

result.show()
