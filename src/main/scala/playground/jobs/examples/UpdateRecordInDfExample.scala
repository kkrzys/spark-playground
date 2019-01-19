package playground.jobs.examples

import org.apache.spark.sql.SparkSession
import playground.jobs.SJob

class UpdateRecordInDfExample(implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {
    import sparkSession.implicits._

    val eplStandingReceiveDf = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/data/landing/topics/dev-eplstanding-receive/*.csv")

    //insert new row to dataframe
    val newRowDf =
      sparkSession.sparkContext.parallelize(
        Seq(("Monaco", 2, 3, null, 1, null, null, null, null, null, 1,
          null, null, null, null, null, null, null))).toDF()

    newRowDf.printSchema()
    newRowDf.show()

    val res = eplStandingReceiveDf.union(newRowDf)

    res.printSchema()
    res.show(50)
  }
}
