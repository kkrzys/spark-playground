package playground.delta_io_example

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import playground.utils.SparkUtils

abstract class DeltaSpec extends FlatSpec with GivenWhenThen with Matchers with DeltaTestUtils {
  implicit val spark = SparkUtils.createSparkSession("delta-test")
}
