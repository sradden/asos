package com.asos.pipeline.staging

import com.asos.pipeline.TestDefaults
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import java.sql.Timestamp
import java.time.LocalDateTime

class RatingStageSpec extends FlatSpec with BeforeAndAfterAll {

  lazy val spark = SparkSession.builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._
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

  "RatingStage" should "write ratings information to the staging area" in {
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

  it should "update the existing rating from userid 7 for movieId 50 from 4.5 to 3.2" in {

    assert(stage.read().filter(
      """
        |userId = 7
        |and movieId = 50
        |and rating = 4.5
        |""".stripMargin
    ).count() == 1)

    stage.upsert(
      Seq((7, 50, 3.2, Timestamp.valueOf(LocalDateTime.now()))).toDF("userId","movieId","rating","timestamp")
    )

    assert(stage.read().filter(
      """
        |userId = 7
        |and movieId = 50
        |and rating = 3.2
        |""".stripMargin
    ).count() == 1)
  }

  it should "insert a new rating from userId 101 for movieId 50" in {
    assert(stage.read().filter(
      """
        |userId = 101
        |and movieId = 50
        |""".stripMargin
    ).count() == 0)

    stage.upsert(
      Seq((101, 50, 9.1, Timestamp.valueOf(LocalDateTime.now()))).toDF("userId","movieId","rating","timestamp")
    )

    assert(stage.read().filter(
      """
        |userId = 101
        |and movieId = 50
        |and rating = 9.1
        |""".stripMargin
    ).count() == 1)

  }
}
