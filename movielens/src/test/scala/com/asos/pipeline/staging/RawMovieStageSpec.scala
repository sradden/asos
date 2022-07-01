package com.asos.pipeline.staging

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class RawMovieStageSpec extends FlatSpec with BeforeAndAfterAll {

  lazy val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()
  lazy val stage = new RawMovieStage()

  var rawMovies = spark.emptyDataFrame

  override def beforeAll(): Unit = {
    rawMovies = stage.read(s"${TestDefaults.SOURCE_PATH}/movies.csv")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      spark.stop()
    }
  }

  "RawMoviesStage" should "read movie information from the specified path" in {
    assert(rawMovies.count().!=(0))
  }

  it should "throw and exception when the specified path does not exist" in {
    intercept[Exception] {
      stage.read(TestDefaults.INVALID_PATH)
    }
  }

  it should "throw an error if the write operation is called" in {
    intercept[NotImplementedError] {
      stage.write(spark.emptyDataFrame)
    }
  }
}
