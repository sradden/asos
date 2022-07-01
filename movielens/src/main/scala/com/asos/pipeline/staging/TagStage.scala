package com.asos.pipeline.staging

import org.apache.spark.sql.functions.{col, from_unixtime, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

class TagStage() extends Stage {
  override def write(data: DataFrame): Unit = ???

  /**
   * reads tag information into a [[DataFrame]] for the path
   * specified at class creation
   * @return
   */
  override def read(path: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(Encoders.product[RawTag].schema)
      .csv(path)
      .transform(forStaging())
  }

  /**
   * transforms [[DataFrame]] into [[Dataset]] of [[Tag]]
   * @return
   */
  private def forStaging(): DataFrame => DataFrame =
    df => {
      df.withColumn("userId", col("userId").cast("int"))
        .withColumn("movieId", col("movieId").cast("int"))
        .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp"))))
    }
}
