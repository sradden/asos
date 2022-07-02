package com.asos.pipeline.staging

import com.asos.pipeline.TestDefaults
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class RawRatingStageSpec extends FlatSpec with BeforeAndAfterAll {

  lazy val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  lazy val stage = new RawRatingStage

  override def afterAll(): Unit =
    try { super.afterAll() }
    finally { spark.stop() }

  "RawRatingsStage" should "read ratings information from the specified path" in {
    assert(stage.read(s"${TestDefaults.SOURCE_PATH}/ratings.csv").count() != 0)
  }

  it should "throw an error when the specified path does not exist" in {
    intercept[Exception] {
      stage.read(TestDefaults.INVALID_PATH)
    }
  }

  it should "throw an error when the unsupported write method is invoked" in {
    intercept[NotImplementedError] {
      stage.write(spark.emptyDataFrame)
    }
  }
}
