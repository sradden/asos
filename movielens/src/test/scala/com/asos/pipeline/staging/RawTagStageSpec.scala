package com.asos.pipeline.staging

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class RawTagStageSpec extends FlatSpec with BeforeAndAfterAll {

  private lazy val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  private lazy val stage = new RawTagStage()

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      spark.stop()
    }
  }

  "RawTagStage" should "read tag information from the specified path" in {
    assert(stage.read(s"${TestDefaults.SOURCE_PATH}/tags.csv").count() != 0)
  }

  it should "throw an error when an invalid path is specified" in {
    intercept[Exception] {
      stage.read(TestDefaults.INVALID_PATH)
    }
  }

  it should "throw an error if the unsupported write method is called" in {
    intercept[NotImplementedError]{
      stage.write(spark.emptyDataFrame)
    }
  }
}
