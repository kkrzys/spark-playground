package playground.jobs.etl

import org.apache.spark.sql.SparkSession
import playground.core.DataContainer
import playground.jobs.SJob

class DistributeSJob(implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {

    import playground.core.DataContainerInstances.Distribute._

    DataContainer.run(footballMatchCompleteContainer, write = true)
    DataContainer.run(eplStandingReceiveContainer, write = true)
  }
}
