package edu.wm.jdeng01

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.io._

/** Find the movies with the most ratings. */
object Question1 {
  
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
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Question1") with Serializable
    
    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)
    
    // Read in each rating line
    val lines = sc.textFile("../ml-100k/u.data")
    
    // Map to (movieID, ratings) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, x.split("\t")(2).toFloat))
    
    // Count up all the ratings for each movie
    val movieCounts = movies.mapValues(x => (x, 1)).reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2))
    
    val movielist = movieCounts.filter( x => x._2._2 > 100)
    
    // Get the average ratings for each movie
    val movieCal = movielist.mapValues( x => (x._1/x._2, x._2))
    
    // Flip (movieID, (avgRating, count)) to ((avgRating, count), movieID)
    val flipped = movieCal.map( x => (x._2, x._1) )
    
    // Sort
    val sortedMovies = flipped.sortByKey()
    
    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
    
    // Collect and print results
    val results = sortedMoviesWithNames.collect()
    
    results.foreach(println)
    
    val file = "Question1.txt"    
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- results) {
        writer.write(x + "\n")
                    }
    writer.close()    
  }  
}

