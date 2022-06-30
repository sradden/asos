package com.asos.pipeline

import com.asos.pipeline.staging.{Stage, StageMovies}
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]): Unit = {

    // todo pass master url and resource paths as cmd line args
    SparkSession
      .builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    ingestionSteps("src/main/resources")
  }

  private def ingestionSteps(path: String): Unit = {

    // triggers the ingestion into staging pipeline
    Seq(new StageMovies(s"$path/movies.csv"))
      .foreach({stage => stage.write(stage.read())} )
  }


}
