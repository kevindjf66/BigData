package edu.wm.

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._
import java.io._

/** Find the movies with the most ratings. */
object Assignment3P1 {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
 
  final case class Movie1(movieID1: Int, Ratings: Int)
  final case class Movie2(movieID2: Int)
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") 
      .getOrCreate()
    
    val lines1 = spark.sparkContext.textFile("../ml-100k/u.data").map(x => Movie1(x.split("\t")(1).toInt, x.split("\t")(2).toInt))
    val lines2 = spark.sparkContext.textFile("../ml-100k/u.data").map(x => Movie2(x.split("\t")(1).toInt))
    
    import spark.implicits._
    val moviesDS1 = lines1.toDS()
    val moviesDS2 = lines2.toDS()
    
    val avgRatings = moviesDS1.groupBy("movieID1").avg("Ratings").cache()
    val topMovieIDs = moviesDS2.groupBy("movieID2").count().cache()
    
    val joined_df = topMovieIDs.join(avgRatings, col("movieID1") === col("movieID2"), "inner")
    
    val final_df = joined_df.select(col("movieID2"), col("count"), col("avg(Ratings)")).filter(joined_df("count") > 100).orderBy(("avg(Ratings)")).cache()
    
    final_df.show()

    val names = loadMovieNames()
    
    val top = final_df.take(374)
    
    val file = "Assignment3P1.txt"    
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (result <- top) {
      writer.write(names(result(0).asInstanceOf[Int]) + ": " + result(2) + ", " + result(1) + "\n")
    }
    writer.close() 
    // Stop the session
    spark.stop()
  }
  
}

