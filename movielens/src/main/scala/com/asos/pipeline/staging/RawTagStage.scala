package com.asos.pipeline.staging

import org.apache.spark.sql.{DataFrame, Encoders}

class RawTagStage extends Stage {

  // raw stages don't implement write functionality
  override def write(data: DataFrame): Unit = throw new NotImplementedError()

  override def read(path: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(Encoders.product[RawTag].schema)
      .csv(path)
  }
}
