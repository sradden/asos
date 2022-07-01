package com.asos.pipeline.transformation

import com.asos.pipeline.staging.Movie
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders}

class TransformMovies {

  def splitGenre(g: Column): DataFrame => DataFrame =
    df => {
      df.withColumn("genre", explode(split(g, "[|]")))
        .as("genre")
    }
}
