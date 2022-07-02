package com.asos.pipeline

import com.asos.pipeline.staging._
import com.asos.pipeline.transformation.TransformMovie
import org.apache.spark.sql.functions.{avg, col, count, round}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * @author ${user.name}
  */
object App {

  val movieStage = new MovieStage
  val ratingStage = new RatingStage

  def main(args: Array[String]): Unit = {

    // todo pass master url and resource paths as cmd line args
    val RESOURCE_PATH = "src/main/resources"
    val OUT_PATH = "out"

    val spark = SparkSession
      .builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    // chain creation of delta tables
    Seq(
      movieStage.write(
        new RawMovieStage().read(s"$RESOURCE_PATH/movies.csv")
      ),
      ratingStage.write(
        new RawRatingStage().read(s"$RESOURCE_PATH/ratings.csv")
      ),
      new TagStage().write(
        new RawTagStage().read(s"$RESOURCE_PATH/tags.csv")
      )
    ).foreach(print(_))

    splitMovieGenrestoParquet(s"$OUT_PATH/split-movie-genres")
    top10FilmsByAvgRatingtoCsv(s"$OUT_PATH/top10-films-by-avg-rating.csv", movieStage.read(), ratingStage.read())
  }

  /**
    * Splits the movie genres so that there is a single genre per row
    * and saves the results to parquet.
    * For example:
    *    Comedy|Romance;
    *
    *    becomes:
    *
    *    Comedy;
    *    Romance;
    *
    * @param path specifies the path where to save the results of
    *             the split op.
    */
  private def splitMovieGenrestoParquet(path: String): Unit = {

    val data = movieStage.read()
      .transform(new TransformMovie().splitGenre(col("genres")))
    // show result of split
    data.filter(col("movieId") === 1).show(false)

    data.write
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  /**
    * Finds the top 10 films by average rating.
    * Each of the top 10 films should have at least 5 ratings to qualify as a top 10 movie.
    * @param movies [[DataFrame]] of [[Movie]]
    * @param ratings [[DataFrame]] of [[Rating]] about a movie
    */
  def top10FilmsByAvgRatingtoCsv(path: String,
      movies: DataFrame,
      ratings: DataFrame
  ): Unit = {
    // get films with 5 or more ratings
    // get the avg rating
    // sortby avg rating desc
    movies.join(ratings.groupBy(col("movieId"))
      .agg(
        count("movieId").as("num_ratings"),
        round(avg("rating"), 2).as("avg_rating")
      ).filter("num_ratings >= 5"), "movieId")
      .orderBy(col("avg_rating").desc)
      .coalesce(1)
      .write
      .option("header", true)
      .option("delimiter", ",")
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
}
