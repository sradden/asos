package com.asos.pipeline.staging

import org.apache.spark.sql.functions.{col, from_unixtime, to_timestamp}
import org.apache.spark.sql.{DataFrame, SaveMode}
import io.delta.tables._

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
      .partitionBy("movieId")
      .save(DELTA_TABLE)
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
    * @param newRating [[DataFrame]] containing a new rating
    */
  def upsert(newRating: DataFrame): Unit = {

    val deltaTblOld = DeltaTable.forPath(DELTA_TABLE)
    deltaTblOld.as("old")
      .merge(
        newRating.as("new"),
        "old.userId = new.userId and old.movieId = new.movieId"
      )
      .whenMatched
      .updateExpr(Map(
        "rating" -> "new.rating",
        "timestamp" -> "new.timestamp"
      ))
      .whenNotMatched
      .insertExpr(Map(
        "userId"-> "new.userId",
        "movieId" -> "new.movieId",
        "rating" -> "new.rating",
        "timestamp" -> "new.timestamp"
      ))
      .execute()
  }

  override def read(path: String = DELTA_TABLE): DataFrame = {
    super.read(DELTA_TABLE)
  }
}
