package playground.etl_example.jobs.etl

import org.apache.spark.sql.SparkSession
import playground.etl_example.core.DataContainer
import playground.etl_example.jobs.SJob

class IngestSJob(implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {

    import playground.etl_example.core.DataContainerInstances.Ingest._

    DataContainer.run(footbalMatchCompleteContainer, write = true)
    DataContainer.run(eplStandingReceiveContainer, write = true)
  }
}
