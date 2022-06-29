package com.asos.pipeline.staging

import java.sql.Date

/**
 * Contains information about a movie
 *
 * @param movieId unique movie id
 * @param title Titles are entered manually or imported from <https://www.themoviedb.org/>,
 *              and include the year of release in parentheses. Errors and inconsistencies may
 *              exist in these titles.
 * @param yearOfRelease optional field that denotes the year the movie was released
 * @param genre Genres are a pipe-separated list
 */
case class StagedMovie(
  movieId: Int,
  title: String,
  yearOfRelease: Int,
  genre: String)
