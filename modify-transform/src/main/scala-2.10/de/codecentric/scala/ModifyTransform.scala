package de.codecentric.scala

import com.datastax.spark.connector._, org.apache.spark.SparkConf, org.apache.spark.SparkContext, org.apache.spark.SparkContext._
import scala.util.Random
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ModifyTransform {
  case class MovieRaw(movieid:Long,genres:String,title:String)
  case class Movie(movieid:Long,genres:Set[String],title:String,year:Int)
  case class Rating (userid:Long, movieid:Long, rating:Double, time: java.util.Date)
  case class Tag(userid:Long,movieid:Long,tag:String,time:java.util.Date)

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //    Create Spark Context
    val conf = new SparkConf(true)
      .setAppName("Modify & Transform")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    val moviesRaw = sc.cassandraTable[MovieRaw]("movie","movies_raw")
    val regexYear = ".*\\((\\d*)\\)".r
    val movies = moviesRaw.map{case MovieRaw(i,g,t) => Movie(i,g.split("\\|").toSet,t.substring(0,t.lastIndexOf('(')).trim,t.trim match { case regexYear(y) => Integer.parseInt(y)})}
    movies.saveToCassandra("movie","movies")

    val ratingsByUser = sc.cassandraTable[Rating]("movie","ratings_by_user")
    ratingsByUser.saveToCassandra("movie","ratings_by_movie")

    val tagsByUser = sc.cassandraTable[Tag]("movie","tags_by_user")
    tagsByUser.saveToCassandra("movie","tags_by_movie")

      }
}
