package com.asos.pipeline.staging

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TagStageSpec extends FlatSpec with BeforeAndAfterAll {

  private lazy val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()
  private lazy val stageTags = new TagStage()
  private val testResourcePath = "src/test/resources/tags.csv"
  var _tags = spark.emptyDataFrame

  override def beforeAll(): Unit = {
    _tags = stageTags.read(testResourcePath)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      spark.stop()
    }
  }

  "StageTags" should "read tag information from the specified path" in {
    assert(_tags.count().!=(0))
  }

  it should "throw and exception when the specified path does not exist" in {
    intercept[Exception] {
      stageTags.read("foobar.csv")
    }
  }

  it should "convert columns to their native data types" in {
    assert(
      _tags.schema("userId").dataType == IntegerType &&
        _tags.schema("movieId").dataType == IntegerType &&
        _tags.schema("tag").dataType == StringType &&
        _tags.schema("timestamp").dataType == TimestampType
    )
  }
}
