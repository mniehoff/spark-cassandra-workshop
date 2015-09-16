package de.codecentric.spark

import com.datastax.spark.connector._, org.apache.spark.SparkConf, org.apache.spark.SparkContext, org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.log4j.Logger
import org.apache.log4j.Level

case class RatingRaw (userid:Long, movieid:Long, rating:Double, time: java.util.Date)

object MachineLearningSimple {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //    Create Spark Context
    val conf = new SparkConf(true)
      .setAppName("Machine Learning Simple")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

  // get ratings and map to mllib Rating case class
	val data = sc.cassandraTable[RatingRaw]("movie","ratings_by_movie")
	val ratings = data.map{case RatingRaw(user,movie,rating,_) => Rating(user.toInt,movie.toInt,rating-2.5)}.cache

	val rank = 20
	val numIterations = 20
	val model = ALS.trainImplicit(ratings, rank, numIterations)

	// Evaluate the model on rating data
	val usersProducts = ratings.map { case Rating(user, product, rate) =>
	  (user, product)
	}
	val predictions =
	  model.predict(usersProducts).map { case Rating(user, product, rate) =>
	    ((user, product), rate)
	  }
	val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
	  ((user, product), rate)
	}.join(predictions)
	val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
	  val err = (r1 - r2)
	  err * err
	}.mean()
	println("Rank: " + rank + ", Iterations: " +  numIterations + ", Mean Squared Error = " + MSE)

  }
}
