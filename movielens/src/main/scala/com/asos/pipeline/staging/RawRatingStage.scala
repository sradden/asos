package com.asos.pipeline.staging

import org.apache.spark.sql.{DataFrame, Encoders}

class RawRatingStage extends Stage {
  // raw stages do not implement write ops so notify callee
  override def write(data: DataFrame): Unit = throw new NotImplementedError()

  override def read(path: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(Encoders.product[RawRating].schema)
      .csv(path)
  }
}
