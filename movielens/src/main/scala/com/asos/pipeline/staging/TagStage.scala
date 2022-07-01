package com.asos.pipeline.staging

import org.apache.spark.sql.functions.{col, from_unixtime, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

class TagStage() extends Stage[Dataset[Tag]] {
  override def write(data: Dataset[Tag]): Unit = ???

  /**
   * reads tag information into a [[Dataset]] for the path
   * specified at class creation
   * @return
   */
  override def read(path: String): Dataset[Tag] = {
    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(Encoders.product[_Tag].schema)
      .csv(path)
      .transform(forStaging())
  }

  /**
   * transforms [[DataFrame]] into [[Dataset]] of [[Tag]]
   * @return
   */
  private def forStaging(): DataFrame => Dataset[Tag] =
    df => {
      df.withColumn("userId", col("userId").cast("int"))
        .withColumn("movieId", col("movieId").cast("int"))
        .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp"))))
        .as[Tag](Encoders.product[Tag])
    }

  case class _Tag(
      userId: String,
      movieId: String,
      tag: String,
      timestamp: String
  )
}
