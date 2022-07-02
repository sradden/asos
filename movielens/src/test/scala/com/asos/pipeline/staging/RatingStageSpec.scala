package com.asos.pipeline.staging

import com.asos.pipeline.TestDefaults
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class RatingStageSpec extends FlatSpec with BeforeAndAfterAll {

  lazy val spark = SparkSession.builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  lazy val stage = new RatingStage()
  var ratings = spark.emptyDataFrame

  override def beforeAll(): Unit = {
    ratings = new RawRatingStage().read(s"${TestDefaults.SOURCE_PATH}/ratings.csv")
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

  "StageRatings" should "write ratings information to the staging area" in {
    stage.write(ratings)
    assert(stage.read().count().!=(0))
  }

  it should "covert columns to their native data types" in {
    val df = stage.read()
    assert{
      df.schema("userId").dataType == IntegerType &&
        df.schema("movieId").dataType == IntegerType &&
        df.schema("rating").dataType == DoubleType &&
        df.schema("timestamp").dataType == TimestampType
    }
  }

  it should "display a rating of 4.0 submitted on 30th July 2007 for Toy Story from userId 1" in {
    assert(stage.read().filter(
      """
        |movieId = 1 and userId = 1
        |and rating = 4.0
        |and to_date(timestamp, 'yyyy-MM-dd') = '2000-07-30'
        |""".stripMargin)
    .count() === 1)
  }
}
