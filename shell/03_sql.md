## SQL Analysis

All exercises in the shell:

```scala
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
val csc = new CassandraSQLContext(sc)
csc.setKeyspace("movie")
```

Every Query is going to be executed like this:

val dataFrame = csc.sql("...query...")

A dataFrame is a special kind of rdd. You can use collect to trigger the execution.
To printout all lines you can use foreach(println) on the result of the collect operation.
Every RDD method can be executed on a dataFrame, i.e filter..

### Some example queries

```sql
Select count(*) from movies
Select * from movies where year = 2000
Select * from movies where title like '%The%'
Select * from movies where year >=1980 and year <=1989
Select max(year) from movies
Select distinct m.title from movies m join tags_by_movie t on t.movieid = m.movieid where t.tag = 'Oscar (Best Picture)'
Select t.tag from movies m join tags_by_movie t on t.movieid = m.movieid where m.title = 'Lord of the Rings: The Two Towers, The'
Select avg(rating) from ratings_by_user
Select count(*)/count(distinct userid) from ratings_by_user
select movieid,avg(rating) as avg_rating from ratings_by_movie group by movieid order by avg_rating desc LIMIT 1
Select userid,count(movieid) AS ratingCount from ratings_by_user group by userid order by ratingCount DESC LIMIT 1
Select userid,count(movieid) AS tagCount from tags_by_user group by userid order by tagCount DESC LIMIT 1
```
