package com.asos.pipeline.staging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

/**
  *
  * Specification testing for [[StageMovies]]
  */
class StageMoviesSpec extends FlatSpec with BeforeAndAfterAll {

  private val testResourcePath = "src/test/resources/movies.csv"
  lazy val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  lazy val stageMovies = new StageMovies(testResourcePath)
  import spark.implicits._
  var movies = spark.emptyDataset[StagedMovie]

  override def beforeAll(): Unit = {
    movies = stageMovies.read()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      spark.stop()
    }
  }

  "StageMovies" should "read movie information from the specified path" in {
    assert(movies.count().!=(0))
  }

  it should "throw and exception when the specified path does not exist" in {
    intercept[Exception] {
      val sm = new StageMovies("foobar.csv")
      sm.read()
    }
  }

  it should "convert columns to their native data types" in {
    assert(
      movies.schema("movieId").dataType == IntegerType
        && movies.schema("title").dataType == StringType
        && movies.schema("yearOfRelease").dataType == IntegerType
        && movies.schema("genre").dataType == StringType
    )
  }

  it should "extract the year of release from the title" in {
    movies
      .filter("movieId = 1 and yearOfRelease = 1995")
      .count() === 1
  }

  it should "display each genre as a separate row" in {
    assert(
      movies.select($"movieId").distinct().count === 25 &&
        movies.filter("movieId = 1").count() === 5
    )
  }
}
