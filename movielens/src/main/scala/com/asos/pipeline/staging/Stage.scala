package com.asos.pipeline.staging

import org.apache.spark.sql.Dataset

/**
 * Trait that allows implementors to stage a specific type
 *
 * @tparam T generic type parameter that denotes a type to be staged
 */
trait Stage[T] {
  def write(data: T) : Unit
  def read(path: String): T
}
