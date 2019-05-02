package playground.core.experimental

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import playground.utils.SparkUtils

import scala.language.higherKinds
import scala.reflect.ClassTag

trait MapableContainer[DataContainer[_]] {
  def map[U, V: ClassTag](in: DataContainer[U])(func: U => V): DataContainer[V]
}

trait FlatableContainer[DataContainer[_]] {
  def flatMap[U, V: ClassTag](in: DataContainer[U])(func: U => Iterable[V]): DataContainer[V]
}

trait FilterableContainer[DataContainer[_]] {
  def filter[U](in: DataContainer[U])(func: U => Boolean): DataContainer[U]
}

trait DataContainerInterpreter[DataContainer[_]] extends MapableContainer[DataContainer]
  with FlatableContainer[DataContainer]
  with FilterableContainer[DataContainer]

class DatasetInterpreter extends DataContainerInterpreter[Dataset] {

  override def map[U, V: ClassTag](in: Dataset[U])(func: U => V): Dataset[V] = {
    implicit val encoder: Encoder[V] = org.apache.spark.sql.Encoders.kryo[V]

    in.map(func)
  }

  override def flatMap[U, V: ClassTag](in: Dataset[U])(func: U => Iterable[V]): Dataset[V] = {
    implicit val encoder: Encoder[V] = org.apache.spark.sql.Encoders.kryo[V]

    in.flatMap(func)
  }

  override def filter[U](in: Dataset[U])(func: U => Boolean): Dataset[U] = {
    in.filter(func)
  }
}

class ListInterpreter extends DataContainerInterpreter[List] {
  override def map[U, V: ClassTag](in: List[U])(func: U => V): List[V] =
    in.map(func)

  override def flatMap[U, V: ClassTag](in: List[U])(func: U => Iterable[V]): List[V] =
    in.flatMap(func)

  override def filter[U](in: List[U])(func: U => Boolean): List[U] =
    in.filter(func)
}

class DataContainerExecutor[DataContainer[_]](dc: DataContainerInterpreter[DataContainer]) {
  def execute(in: DataContainer[String]): DataContainer[Integer] = {
    dc.map(in)(el => el.length)
  }
}

object DataContainerMain extends App {
  implicit val spark: SparkSession = SparkUtils.createSparkSession("TaglessFinal")

  import spark.implicits._

  val l: List[String] = List("one", "two", "three")
  val ds: Dataset[String] = Seq("one", "two", "three").toDS()

  val resultDs = new DataContainerExecutor(new DatasetInterpreter).execute(ds)
  val resultL = new DataContainerExecutor(new ListInterpreter).execute(l)

  println(resultDs.collect().toList)
  println(resultL)
}
