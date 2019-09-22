package playground.delta_io_example

import io.delta.tables.DeltaTable

class DeltaExampleTest extends DeltaSpec {
  "Delta table" should "be updated with new values" in {
    withTempDir { dir =>
      Given("EPL standings data written to table and current data before updating")
      val data = spark.read.option("header", "true")
        .csv("src/main/resources/data/landing/topics/dev-eplstanding-receive/*.csv")
      data.write.format("delta").mode("overwrite").save(dir.getAbsolutePath)

      val currentData = data.where("Team = 'Arsenal'")
        .select("2000", "2001").collect().toList.head
      currentData.getString(0) shouldBe "2"
      currentData.getString(1) shouldBe "2"

      When("updating data with new values")
      val deltaTable = DeltaTable.forPath(spark, dir.getAbsolutePath)
      deltaTable.updateExpr(
        "Team = 'Arsenal'",
        Map("2000" -> "'4'", "2001" -> "'4'"))

      Then("rows should be updated with new/expected values")
      val result = deltaTable
        .toDF
        .where("Team = 'Arsenal'")
        .select("2000", "2001").collect().toList.head

      result.getString(0) shouldBe "4"
      result.getString(1) shouldBe "4"
    }
  }
}
