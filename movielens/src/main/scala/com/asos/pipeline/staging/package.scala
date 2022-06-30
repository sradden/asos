package com.asos.pipeline

import java.sql.Timestamp

package object staging {

  case class Ratings(
                      userId: Int,
                      movieId: Int,
                      rating: Double,
                      timestamp: Timestamp
                    )
}
