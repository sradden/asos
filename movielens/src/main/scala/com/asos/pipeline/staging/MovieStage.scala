package com.asos.pipeline.staging

import org.apache.spark.sql.functions.{col, explode, regexp_extract, split}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode}

class MovieStage() extends Stage {

  override def write(data: DataFrame): Unit = {
    data
      .transform(forStaging())
      .write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .save("spark-warehouse/delta/movies-bronze")
  }

  /**
    * asStagedMovie performs a series of transformations on the specified [[DataFrame]] to
    * convert it into a [[Dataset[StagedMovie]]
    * @return
    */
  private def forStaging(): DataFrame => Dataset[Movie] =
    df => {
      df.withColumn("movieId", col("movieId").cast("int"))
        .withColumn(
          "yearOfRelease",
          regexp_extract(col("title"), "(\\d+)", 1).cast("int")
        )
        .withColumn("genre", explode(split(col("genre"), "[|]")))
        .as("genre")
        .as[Movie](Encoders.product[Movie])
    }

  /**
    * Reads information about movies in a [[DataFrame]]
    * @return a [[DataFrame] ready to be written to the staging area.
    */
  override def read(path: String = "spark-warehouse/delta/movies-bronze"): DataFrame = {
    // todo filter filepath already ingested to detect new files only
    super.read(path)
  }
}
