package com.asos.pipeline.staging

import org.apache.spark.sql.functions.{col, from_unixtime, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

class RatingStage() extends Stage[Dataset[Rating]] {

  override def write(data: Dataset[Rating]): Unit = ???

  override def read(path: String): Dataset[Rating] = {

    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(Encoders.product[this._Rating].schema)
      .csv(path)
      .transform(forStaging())
  }

  private def forStaging(): DataFrame => Dataset[Rating] =
    df => {
      df.withColumn("userId", col("userId").cast("int"))
        .withColumn("movieId", col("movieId").cast("int"))
        .withColumn("rating", col("rating").cast("double"))
        .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp"))))
        .as[Rating](Encoders.product[Rating])
    }

  /**
    * Case class receives the raw ratings format read from path
    * @param userId Indentifier of the user supplying the rating.
    * @param movieId Identifier of the movie being rated.
    * @param rating Ratings are made on a 5-star scale, with half-star increments (0.5 stars - 5.0 stars).
    * @param timestamp Timestamps represent seconds since midnight Coordinated Universal Time (UTC) of January 1, 1970.
    */
  private case class _Rating(
      userId: String,
      movieId: String,
      rating: String,
      timestamp: String
  )
}
