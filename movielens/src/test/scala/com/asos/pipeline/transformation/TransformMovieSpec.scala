package com.asos.pipeline.transformation

import com.asos.pipeline.TestDefaults
import com.asos.pipeline.staging.{MovieStage, RawMovieStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
class TransformMovieSpec extends FlatSpec with BeforeAndAfterAll {

  lazy val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  private var movies = spark.emptyDataFrame

  override def beforeAll(): Unit = {
    val stage = new MovieStage()

    stage.write(
      new RawMovieStage().read(s"${TestDefaults.SOURCE_PATH}/movies.csv")
    )

    movies = stage.read()

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try { super.afterAll() }
    finally { spark.stop() }
  }

  "TransformMovie" should "should split multiple genres per movie as a separate row" in {
    val df = movies.transform(new TransformMovie().splitGenre(col("genres")))
    assert(
      df.select(col("movieId"))
        .distinct()
        .count === 25 &&
        df.filter("movieId = 1").count() === 5
    )
  }

  it should "rename the genres column as genre" in {
    val df = movies.transform(new TransformMovie().splitGenre(col("genres")))
    assert(df.schema("genre").dataType == StringType)
  }
}
