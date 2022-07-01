package com.asos.pipeline

import java.sql.Timestamp

package object staging {

  /**
    * Contains user ratings for a movie
    * @param userId unique user id
    * @param movieId movie id
    * @param rating the rating for the movie
    * @param timestamp the epoch when the rating was given
    */
  case class Rating(
      userId: Int,
      movieId: Int,
      rating: Double,
      timestamp: Timestamp
  )

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
  case class Movie(
      movieId: Int,
      title: String,
      yearOfRelease: Int,
      genre: String
  )

  /**
    * A tag is user-generated metadata (typically a single word or short phrase) about a movie applied by a user.
    *
    * @param userId id of the user who applied the tag
    * @param movieId id of the movie the tag applies to
    * @param tag metadata about the movie
    * @param timestamp epoch since midnight Coordinated Universal Time (UTC) of January 1, 1970.
    */
  case class Tag(
      userId: Int,
      movieId: Int,
      tag: String,
      timestamp: Timestamp
  )

  /**
    * structure of the movie information when read from the supplied path
    * @param movieId
    * @param title
    * @param genre
    */
  case class RawMovie(movieId: String, title: String, genre: String)

  /**
    * structure of a tag when read from supplied path
    * @param userId
    * @param movieId
    * @param tag
    * @param timestamp
    */
  case class RawTag(
      userId: String,
      movieId: String,
      tag: String,
      timestamp: String
  )
}
