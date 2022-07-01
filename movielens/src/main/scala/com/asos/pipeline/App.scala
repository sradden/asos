package com.asos.pipeline

import com.asos.pipeline.staging._
import com.asos.pipeline.transformation.TransformMovies
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]): Unit = {

    // todo pass master url and resource paths as cmd line args
    SparkSession
      .builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    val path = "src/main/resources"
    val movies = stageMovies(path)
    stageTags(path)
    splitMovieGenres(movies)
    top10FilmsByAvgRating(movies, stageRatings(path))
  }

  private def stageMovies(path: String): DataFrame = {
    val stageMovies = new MovieStage()
    val movies = stageMovies.read(s"$path/movies.csv")
    stageMovies.write(movies)
    movies
  }

  private def stageTags(path: String): DataFrame = {
    val stageTags = new TagStage()
    val tags = stageTags.read(s"$path/tags.csv")
    stageTags.write(tags)
    tags
  }

  private def stageRatings(path: String): DataFrame = {
    val stageRatings = new RatingStage()
    val ratings = stageRatings.read(s"$path/ratings.csv")
    stageRatings.write(ratings)
    ratings
  }

  private def splitMovieGenres(movies: DataFrame): DataFrame = {
    movies.transform(new TransformMovies().splitGenre(col("genre")))
  }

  /**
   * Finds the top 10 films by average rating.
   * Each of the top 10 films should have at least 5 ratings to qualify as a top 10 movie.
   * @param movies [[Dataset]] of [[Movie]]
   * @param ratings [[Dataset]] of [[Rating]]
   * @return top 10 films ordered by the highest avg rating descending
   */
  private def top10FilmsByAvgRating(
      movies: DataFrame,
      ratings: DataFrame
  ): DataFrame = ???
}
