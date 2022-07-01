package com.asos.pipeline.staging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

/**
  *
  * Specification testing for [[MovieStage]]
  */
class MovieStageSpec extends FlatSpec with BeforeAndAfterAll {

  private val DELTA_TABLE_NAME = "spark-warehouse/delta/movies-bronze"
  private val MOVIE_SRC_PATH = ""

  lazy val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  lazy val stage = new MovieStage()
  import spark.implicits._
  var movies = spark.emptyDataFrame

  override def beforeAll(): Unit = {
    movies = new RawMovieStage().read(s"${TestDefaults.SOURCE_PATH}/movies.csv")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      spark.stop()
    }
  }

  it should "write movie data to the staging area" in {
    stage.write(movies)
    assert(stage.read().count() != 0)
  }

  it should "have converted columns to their native data types" in {
    val df = stage.read()
    assert(
      df.schema("movieId").dataType == IntegerType
        && df.schema("title").dataType == StringType
        && df.schema("yearOfRelease").dataType == IntegerType
        && df.schema("genre").dataType == StringType
    )
  }

  it should "extract the year of release from the title" in {
    stage.read()
      .filter("movieId = 1 and yearOfRelease = 1995")
      .count() === 1
  }
/*
  //todo refactor to TransformMovies
  it should "display each genre as a separate row" in {
    assert(
      movies.select($"movieId").distinct().count === 25 &&
        movies.filter("movieId = 1").count() === 5
    )
  }
*/
}
