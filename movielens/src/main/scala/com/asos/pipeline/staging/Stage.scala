package com.asos.pipeline.staging

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Trait that allows implementors to stage a [[DataFrame]]
  * @tparam DataFrame the [[DataFrame]] to be staged
  */
trait Stage {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  def write(data: DataFrame): Unit
  // default implementation reads from delta table specified in path
  def read(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }
}
