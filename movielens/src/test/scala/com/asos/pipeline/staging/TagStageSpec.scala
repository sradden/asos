package com.asos.pipeline.staging

import com.asos.pipeline.TestDefaults
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TagStageSpec extends FlatSpec with BeforeAndAfterAll {

  private lazy val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  private lazy val stage = new TagStage()
  var _tags = spark.emptyDataFrame

  override def beforeAll(): Unit = {
    _tags = new RawTagStage().read(s"${TestDefaults.SOURCE_PATH}/tags.csv")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      spark.stop()
    }
  }

  "TagStage" should "write tag information to the staging area" in {
    stage.write(_tags)
    assert(stage.read().count().!=(0))
  }

  it should "convert columns to their native data types" in {
    val df = stage.read()
    assert(
      df.schema("userId").dataType == IntegerType &&
        df.schema("movieId").dataType == IntegerType &&
        df.schema("tag").dataType == StringType &&
        df.schema("timestamp").dataType == TimestampType
    )
  }
}