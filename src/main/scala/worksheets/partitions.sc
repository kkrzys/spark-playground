import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import playground.utils.SparkUtils

implicit val spark: SparkSession = SparkUtils.createSparkSession("spark")
implicit val sc: SparkContext = spark.sparkContext

import spark.implicits._

val df = Seq(("USA", 1, 1), ("USA", 11, 11), ("Poland", 2, 2), ("England", 3, 3), ("Ukraine", 44, 44),
  ("Ukraine", 4, 4), ("Ukraine", 444, 444)).toDF("Country", "N1", "N2")

df.repartition(4)
  .write.format("parquet").mode(SaveMode.Overwrite)
  .partitionBy("Country")
  .save("partitions")

val res = spark.read.format("parquet").load("partitions")

println(s"Default parallelism: ${sc.defaultParallelism}")
println(s"Num of partitions: ${res.rdd.partitions.length}")

res.foreachPartition((it: Iterator[Row]) => {
  println(it.toList)
})
