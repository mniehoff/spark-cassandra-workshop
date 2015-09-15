Scala Spark Applications
- Streaming
- Machine Learning Simple
- Machine Learning Advanced (with user input)

To start:

```sbt assembly```

```spark-submit --class <<ClassName>> target/scala-2.10/spark-scala-assembly-1.0.jar --master spark://<<your-fully-qualified-host:7077 ```

Requires a Spark Cluster with enough resources.

I.e: set driver memory and executor memory to at least 3g (for recommendation)

Driver Memory: set in conf/spark-default.conf or as --driver-memory 3g on the spark-submit
Executor Memory: set in conf/spark-default.conf or as spark.executor.memory in your app (SparkConf())
