package de.codecentric.spark

import com.datastax.spark.connector._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import scala.util.Random
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Date

/**
 *
 */
object Streaming {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NetworkWordCount").set("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    val withDate = wordCounts.map(tuple => (new Date(), tuple._1, tuple._2))
    withDate.foreachRDD(_.saveToCassandra("movie","streamcount"))
    ssc.start()
    ssc.awaitTermination()
  }
}
