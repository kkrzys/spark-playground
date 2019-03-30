package playground.jobs.etl

import org.apache.spark.sql.SparkSession
import playground.api.Container
import playground.jobs.SJob

class DistributeSJob(implicit sparkSession: SparkSession) extends SJob {
  override def execute(): Unit = {

    import playground.api.ContainerInstances.Distribute._

    Container.run(footballMatchCompleteContainer, write = true)
    Container.run(eplStandingReceiveContainer, write = true)
  }
}
