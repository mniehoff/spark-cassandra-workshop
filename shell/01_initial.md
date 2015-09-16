# read file & word count

```scala
val readmeFile = sc.textFile("spark-1.4.1/README.md")
val linesWithSpark = readmeFile.filter(e => e.contains("Spark"))
linesWithSpark.toDebugString
val count = linesWithSpark.count

val changesFile = sc.textFile("spark-1.4.1/CHANGES.txt")

val readmeCount = readmeFile.flatMap(line => line.split(" ")).map(word => (word,1))
val changesCount = changesFile.flatMap(line => line.split(" ")).map(word => (word,1))

val complete = readmeCount.union(changesCount).reduceByKey(_+_).sortBy(-_._2)
complete.take(10)
```

# computing pi
```scala
val NUM_SAMPLES = 1000000000000000
val count = sc.parallelize(1 to NUM_SAMPLES).map{i =>
  val x = Math.random()
  val y = Math.random()
  if (x*x + y*y < 1) 1 else 0
}.reduce(_ + _)
println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)
