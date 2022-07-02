package com.asos.pipeline.staging

import org.apache.spark.sql.functions.{col, from_unixtime, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

class TagStage() extends Stage {

  private val DELTA_TABLE = "out/delta/tags-bronze"
  override def write(data: DataFrame): Unit = {
    data
      .transform(forStaging())
      .write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .save(DELTA_TABLE)
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

  /**
    * reads tag information into a [[DataFrame]] for the path
    * specified at class creation
    * @return
    */
  override def read(path: String = DELTA_TABLE): DataFrame = {
    super.read(DELTA_TABLE)
  }
}
