Spark ETL Job for moviedb data

To start:

```sbt assembly```

```spark-submit --class de.codecentric.scala.ModifyTransform target/scala-2.10/modify-transform-assembly-1.0.jar --master spark://<<your-fully-qualified-host>>:7077 ```
