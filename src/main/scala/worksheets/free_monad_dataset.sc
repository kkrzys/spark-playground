import cats.free.Free
import cats.free.Free.liftF
import cats.{Id, ~>}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import playground.utils.SparkUtils

sealed trait SOperationT[A]

case class DataSetTransformation[A, B](ds: Dataset[A], t: Dataset[A] => Dataset[B]) extends SOperationT[Dataset[B]]

type SOperation[T] = Free[SOperationT, T]

def transformDs[A, B](df: Dataset[A], t: Dataset[A] => Dataset[B]): SOperation[Dataset[B]] =
  liftF[SOperationT, Dataset[B]](DataSetTransformation(df, t))

//---------------------------------------------------------------------------

implicit val spark: SparkSession = SparkUtils.createSparkSession("spark")
implicit val sc: SparkContext = spark.sparkContext

import spark.implicits._

case class CountryInfo(name: String, n1: Int, n2: Int)
case class CountryInfo2(name: String, n1: Int)
case class CountryInfo3(countryName: String)

val ds = Seq(CountryInfo("USA", 1, 1),
  CountryInfo("USA", 11, 11), CountryInfo("Poland", 2, 2),
  CountryInfo("England", 3, 3), CountryInfo("Ukraine", 44, 44),
  CountryInfo("Ukraine", 4, 4), CountryInfo("Ukraine", 444, 444)).toDS()

def program: SOperation[Dataset[_]] =
  for {
    ds1 <- transformDs(ds, (el: Dataset[CountryInfo]) => el.map(c => CountryInfo2(c.name, c.n1)))
    resDs <- transformDs(ds1, (el: Dataset[CountryInfo2]) => el.map(c => CountryInfo3(c.name)))
  } yield resDs

//------------------------------------------------------------------------------

def dataSetCompiler: SOperationT ~> Id =
  new (SOperationT ~> Id) {
    override def apply[A](fa: SOperationT[A]): Id[A] = {
      fa match {
        case DataSetTransformation(dataSet, t) => t(dataSet).asInstanceOf[A]
      }
    }
  }

//---------------------------------------------------------------------------

val result: Dataset[_] = program.foldMap(dataSetCompiler)

result.show()
