package com.asos.pipeline.staging

import org.apache.spark.sql.functions.{col, explode, regexp_extract, split}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

class MovieStage() extends Stage[Dataset[Movie]] {

  override def write(data: Dataset[Movie]): Unit = {
    data.write
      .format("delta")
      .save("spark-warehouse/delta/movies-bronze")
  }

  /**
    * Reads information about movies in a [[Dataset]]
    * @return a [[Dataset[StagedMovie]] ready to be written to the staging area.
    */
  override def read(path: String): Dataset[Movie] = {
    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(Encoders.product[_Movie].schema)
      .csv(path)
      .transform(forStaging())

    // todo filter filepath already ingested to detect new files only
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
    * structure of the movie information when read from the supplied path
    * @param movieId
    * @param title
    * @param genre
    */
  private case class _Movie(movieId: String, title: String, genre: String)
}
