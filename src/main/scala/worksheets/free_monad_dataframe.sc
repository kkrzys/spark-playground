import cats.free.Free
import cats.free.Free.liftF
import cats.{Id, ~>}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import playground.utils.SparkUtils

sealed trait SOperationA[A]

case class DataFrameTransformation(df: DataFrame, t: DataFrame => DataFrame) extends SOperationA[DataFrame]

type SOperation[A] = Free[SOperationA, A]

def transformDf(df: DataFrame, t: DataFrame => DataFrame): SOperation[DataFrame] =
  liftF[SOperationA, DataFrame](DataFrameTransformation(df, t))

//-------------------------------------------------------------------------

implicit val spark: SparkSession = SparkUtils.createSparkSession("spark")
implicit val sc: SparkContext = spark.sparkContext

import spark.implicits._

val df = Seq(("USA", 1, 1), ("USA", 11, 11), ("Poland", 2, 2), ("England", 3, 3), ("Ukraine", 44, 44),
  ("Ukraine", 4, 4), ("Ukraine", 444, 444)).toDF("Country", "N1", "N2")

def program: SOperation[DataFrame] =
  for {
    df1 <- transformDf(df, _.select("Country", "N1"))
    resDf <- transformDf(df1, _.select("Country").withColumnRenamed("Country", "C"))
  } yield resDf

//--------------------------------------------------------------------------

def dataFrameCompiler: SOperationA ~> Id =
  new (SOperationA ~> Id) {
    override def apply[A](fa: SOperationA[A]): Id[A] = {
      fa match {
        case DataFrameTransformation(dataFrame, t) => t(dataFrame).asInstanceOf[A]
      }
    }
  }

//---------------------------------------------------------------------------

val result: DataFrame = program.foldMap(dataFrameCompiler)

result.show()
