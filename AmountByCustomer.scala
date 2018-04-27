package edu.wm.jdeng01

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.io._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object TotalAmount {
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers
      val customer = fields(1).toInt
      val AmountSpent = fields(3).toInt
      // Create a tuple that is our result.
      (customer, AmountSpent)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalAmount")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../DataA1.csv")
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    val totalsByAmount = rdd.reduceByKey( (x, y) => x + y)
    
    val sortedCustomers = totalsByAmount.sortByKey()
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = totalsByAmount.collect()
    results.foreach(println)
    
    val file = "part1.txt"    
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- results) {
        writer.write(x + "\n")
                    }
    writer.close()
  }
}

  