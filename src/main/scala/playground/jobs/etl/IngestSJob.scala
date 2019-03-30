package playground.jobs.etl

import org.apache.spark.sql.SparkSession
import playground.core.DataContainer
import playground.jobs.SJob

class IngestSJob(implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {

    import playground.core.DataContainerInstances.Ingest._

    DataContainer.run(footbalMatchCompleteContainer, write = true)
    DataContainer.run(eplStandingReceiveContainer, write = true)
  }
}
