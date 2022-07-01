package com.asos.pipeline.staging

import org.apache.spark.sql.{DataFrame, Encoders}

class RawMovieStage extends Stage[DataFrame] {
  // raw movies are not saved therefore don't implement this method
  override def write(data: DataFrame): Unit = throw new NotImplementedError()

  override def read(path: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(Encoders.product[RawMovie].schema)
      .csv(path)
  }
}
