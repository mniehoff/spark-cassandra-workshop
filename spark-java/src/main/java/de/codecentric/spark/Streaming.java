package de.codecentric.spark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapTupleToRow;

import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import scala.Tuple3;

public class Streaming {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").setAppName("Streaming APP");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")));
		JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
		JavaDStream<Tuple3<Date, String, Integer>> withDate = wordCounts
				.map(tuple -> new Tuple3<>(new Date(), tuple._1, tuple._2));
		withDate.foreachRDD(rdd -> {
			javaFunctions(rdd).writerBuilder("movie", "streamcount", mapTupleToRow(Date.class, String.class, Integer.class))
					.saveToCassandra();
			return null;
		});
		jsc.start();
		jsc.awaitTermination();
	}
}
