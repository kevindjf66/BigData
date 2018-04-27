package edu.wm.jdeng01

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.io._

object Question2 {

  def sort_friends(friends: List[(Int, Int)]) : List[Int]   = {

      friends.sortBy(tup_pair => (-tup_pair._2, tup_pair._1)).map(tup_pair => tup_pair._1)

  }
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Question2").setMaster("local")

    val sc = new SparkContext(conf)
    
    val data = sc.textFile("../input-f.txt")

    val user_friend = data.map(line=>line.split("\\t"))
    
    val pairs = user_friend.filter(line => (line.size == 2))

    val pairs_friends = pairs.map(line=>(line(0),line(1).split(",")))
    
    val mutual_pairs = pairs_friends.flatMap(x=>x._2.flatMap(z=>Array((x._1.toInt,z.toInt))))

    val SelfJoin = mutual_pairs.join(mutual_pairs)

    val allFriends = SelfJoin.map(elem => elem._2).filter(elem => elem._1 != elem._2)

    val MutualFriends = allFriends.subtract(mutual_pairs)

    val pair_friends = MutualFriends.map(x => (x, 1))

    val recommended_friend = pair_friends.reduceByKey((a, b) => a + b).map(elem => (elem._1._1, (elem._1._2, elem._2))).
    groupByKey().map(triplet => (triplet._1, sort_friends(triplet._2.toList.take(10)))).
    map(triplet =>"\n" + triplet._1.toString + "\t" + triplet._2.map(x => x.toString).toArray.mkString(",")).collect.mkString(",")
    
    val file = "Question2.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- recommended_friend) {
      writer.write(x)
    }
    writer.close()
  }
}