package com.asos.pipeline.staging

import org.apache.spark.sql.functions.{col, from_unixtime, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode}

import java.sql.Timestamp

class RatingStage() extends Stage {

  // todo config class
  private val DELTA_TABLE = "out/delta/ratings-bronze"

  override def write(data: DataFrame): Unit = {
    // todo filter filepath already ingested to detect new files only using filename function

    data
      .transform(forStaging())
      .write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .save(DELTA_TABLE)
  }

  override def read(path: String = DELTA_TABLE): DataFrame = {
    super.read(DELTA_TABLE)
  }

  private def forStaging(): DataFrame => DataFrame =
    df => {
      df.withColumn("userId", col("userId").cast("int"))
        .withColumn("movieId", col("movieId").cast("int"))
        .withColumn("rating", col("rating").cast("double"))
        .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp"))))
    }

  /**
   * performs an upsert to the rating staging table.
   * using userId and movieId as the primary key it updates an existing row if it exists
   * otherwise inserts a new row
   * @param userId the id of the user giving the rating
   * @param movieId the id of the movie being rated
   * @return a [[DataFrame]] with the result of the upsert
   */
  def upsert(userId: Int, movieId: Int, values: (Double, Timestamp)): DataFrame = ???
}
