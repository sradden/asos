package com.asos.pipeline.staging

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Trait that allows implementors to stage a specific type
 *
 * @tparam T generic type parameter that denotes a type to be staged
 */
trait Stage[T] {
  lazy val spark = SparkSession.builder().getOrCreate()
  def write(data: T) : Unit
  def read(): T
}
