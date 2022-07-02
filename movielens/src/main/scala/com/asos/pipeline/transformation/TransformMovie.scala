package com.asos.pipeline.transformation

import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.{Column, DataFrame}

class TransformMovie {

  /**
   * Splits the genres column into a row for each genre.
   * @param g the genres column
   * @return a new column called genre that contains the result of the split op
   */
  def splitGenre(g: Column): DataFrame => DataFrame =
    df => {
      df.withColumn("genre", explode(split(g, "[|]")))
        .drop(g)
    }
}
