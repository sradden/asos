package com.asos.pipeline.staging

import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, explode, regexp_extract, split}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, SparkSession}

private case class Movie (movieId: String, title: String, genre: String)

class StageMovies() extends Stage[Dataset[StagedMovie]] {

  private lazy val spark = SparkSession.builder().master("local").getOrCreate()

  override def write(data: Dataset[StagedMovie]): Unit = ???

  override def read(path: String): Dataset[StagedMovie] = {
    spark
      .read
      .option("header", true)
      .option("delimiter", ",")
      .schema(Encoders.product[Movie].schema)
      .csv(path)
      .transform(asStagedMovie())
  }

  /**
   * asStagedMovie performs a series of transformations on the specified [[DataFrame]] to
   * convert it into a [[Dataset[StagedMovie]]
   * @return
   */
  private def asStagedMovie(): DataFrame => Dataset[StagedMovie] =
    df => {
      df.withColumn("movieId", col("movieId").cast("int"))
        .withColumn("yearOfRelease",
          regexp_extract(col("title"), "(\\d+)", 1).cast("int"))
        .withColumn("genre", explode(split(col("genre"), "[|]"))).as("genre")
        .as[StagedMovie](Encoders.product[StagedMovie])
    }
}
