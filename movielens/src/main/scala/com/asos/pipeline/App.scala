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

  private def stageMovies(path: String): Dataset[Movie] = {
    val stageMovies = new StageMovies(s"$path/movies.csv")
    val movies = stageMovies.read()
    stageMovies.write(movies)
    movies
  }

  private def stageTags(path: String): Dataset[staging.Tag] = {
    val stageTags = new StageTags(s"$path/tags.csv")
    val tags = stageTags.read()
    stageTags.write(tags)
    tags
  }

  private def stageRatings(path: String): Dataset[Rating] = {
    val stageRatings = new StageRatings(s"$path/ratings.csv")
    val ratings = stageRatings.read()
    stageRatings.write(ratings)
    ratings
  }

  private def splitMovieGenres(movies: Dataset[Movie]): Dataset[Movie] = {
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
      movies: Dataset[Movie],
      ratings: Dataset[Rating]
  ): DataFrame = ???
}
