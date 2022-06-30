package com.asos.pipeline.staging

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class StageRatingsSpec extends FlatSpec with BeforeAndAfterAll {

  private val testResourcePath = "src/test/resources/ratings.csv"
  lazy val spark = SparkSession.builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  lazy val stageRatings = new StageRatings(testResourcePath)

  import spark.implicits._
  var ratings = spark.emptyDataset[Rating]

  override def beforeAll(): Unit = {
    ratings = stageRatings.read()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try{
      super.afterAll()
    }
    finally {
      spark.stop()
    }
  }

  "StageRatings" should "read rating information from the specified path" in {
    assert(ratings.count().!=(0))
  }

  it should "throw and exception when the specified path does not exist" in {
    intercept[Exception] {
      new StageRatings("foobar.csv").read()
    }
  }

  it should "covert columns to their native data types" in {
    assert{
      ratings.schema("userId").dataType == IntegerType &&
        ratings.schema("movieId").dataType == IntegerType &&
        ratings.schema("rating").dataType == DoubleType &&
        ratings.schema("timestamp").dataType == TimestampType
    }
  }

  it should "display a rating of 4.0 submitted on 30th July 2007 for Toy Story from userId 1" in {
    assert(ratings.filter(
      """
        |movieId = 1 and userId = 1
        |and rating = 4.0
        |and to_date(timestamp, 'yyyy-MM-dd') = '2000-07-30'
        |""".stripMargin)
    .count() === 1)
  }
}
