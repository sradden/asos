package com.asos.pipeline

import com.asos.pipeline.staging.StageMovies

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    val data = new StageMovies().read("src/main/resources/movies.csv")
    data.printSchema()
    data.show(false)
  }

}
