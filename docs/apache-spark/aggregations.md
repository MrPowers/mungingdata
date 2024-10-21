---
title: "Aggregations with Spark (groupBy, cube, rollup)"
date: "2019-02-25"
categories: 
  - "apache-spark"
---

# Aggregations with Spark (groupBy, cube, rollup)

Spark has a variety of aggregate functions to group, cube, and rollup DataFrames.

This post will explain how to use aggregate functions with Spark.

Check out [Beautiful Spark Code](https://leanpub.com/beautiful-spark/) for a detailed overview of how to structure and test aggregations in production applications.

## groupBy()

Let's create a DataFrame with two famous soccer players and the number of goals they scored in some games.

```
val goalsDF = Seq(
  ("messi", 2),
  ("messi", 1),
  ("pele", 3),
  ("pele", 1)
).toDF("name", "goals")
```

Let's inspect the contents of the DataFrame:

```
goalsDF.show()

+-----+-----+
| name|goals|
+-----+-----+
|messi|    2|
|messi|    1|
| pele|    3|
| pele|    1|
+-----+-----+
```

Let's use `groupBy()` to calculate the total number of goals scored by each player.

```
import org.apache.spark.sql.functions._

goalsDF
  .groupBy("name")
  .agg(sum("goals"))
  .show()
```

```
+-----+----------+
| name|sum(goals)|
+-----+----------+
| pele|         4|
|messi|         3|
+-----+----------+
```

We need to import `org.apache.spark.sql.functions._` to access the `sum()` method in `agg(sum("goals")`. There are a ton of aggregate functions defined in the [functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) object.

The `groupBy` method is defined in the [Dataset](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) class. `groupBy` returns a [RelationalGroupedDataset](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset) object where the `agg()` method is defined.

Spark makes great use of object oriented programming!

The `RelationalGroupedDataset` class also defines a `sum()` method that can be used to get the same result with less code.

```
goalsDF
  .groupBy("name")
  .sum()
  .show()
```

```
+-----+----------+
| name|sum(goals)|
+-----+----------+
| pele|         4|
|messi|         3|
+-----+----------+
```

[Testing Spark Applications](https://leanpub.com/testing-spark/) teaches you how to package this aggregation in a custom transformation and write a unit test. You should read the book if you want to fast-track you Spark career and become an expert quickly.

## groupBy() with two arguments

Let's create another DataFrame with information on students, their country, and their continent.

```
val studentsDF = Seq(
  ("mario", "italy", "europe"),
  ("stefano", "italy", "europe"),
  ("victor", "spain", "europe"),
  ("li", "china", "asia"),
  ("yuki", "japan", "asia"),
  ("vito", "italy", "europe")
).toDF("name", "country", "continent")
```

Let's get a count of the number of students in each continent / country.

```
studentsDF
  .groupBy("continent", "country")
  .agg(count("*"))
  .show()
```

```
+---------+-------+--------+
|continent|country|count(1)|
+---------+-------+--------+
|   europe|  italy|       3|
|     asia|  japan|       1|
|   europe|  spain|       1|
|     asia|  china|       1|
+---------+-------+--------+
```

We can also leverage the `RelationalGroupedDataset#count()` method to get the same result:

```
studentsDF
  .groupBy("continent", "country")
  .count()
  .show()
```

```
+---------+-------+-----+
|continent|country|count|
+---------+-------+-----+
|   europe|  italy|    3|
|     asia|  japan|    1|
|   europe|  spain|    1|
|     asia|  china|    1|
+---------+-------+-----+
```

## groupBy() with filters

Let's create another DataFrame with the number of goals and assists for two hockey players during a few seasons:

```
val hockeyPlayersDF = Seq(
  ("gretzky", 40, 102, 1990),
  ("gretzky", 41, 122, 1991),
  ("gretzky", 31, 90, 1992),
  ("messier", 33, 61, 1989),
  ("messier", 45, 84, 1991),
  ("messier", 35, 72, 1992),
  ("messier", 25, 66, 1993)
).toDF("name", "goals", "assists", "season")
```

Let's calculate the average number of goals and assists for each player in the 1991 and 1992 seasons.

```
hockeyPlayersDF
  .where($"season".isin("1991", "1992"))
  .groupBy("name")
  .agg(avg("goals"), avg("assists"))
  .show()
```

```
+-------+----------+------------+
|   name|avg(goals)|avg(assists)|
+-------+----------+------------+
|messier|      40.0|        78.0|
|gretzky|      36.0|       106.0|
+-------+----------+------------+
```

Now let's calculate the average number of goals and assists for each player with more than 100 assists on average.

```
hockeyPlayersDF
  .groupBy("name")
  .agg(avg("goals"), avg("assists").as("average_assists"))
  .where($"average_assists" >= 100)
  .show()
```

```
+-------+------------------+------------------+
|   name|        avg(goals)|   average_assists|
+-------+------------------+------------------+
|gretzky|37.333333333333336|104.66666666666667|
+-------+------------------+------------------+
```

Many SQL implementations use the `HAVING` keyword for filtering after aggregations. The same Spark `where()` clause works when filtering both before and after aggregations.

## cube()

> `cube` isn't used too frequently, so feel free to skip this section.

Let's create another sample dataset and replicate the `cube()` examples in [this Stackoverflow answer](https://stackoverflow.com/a/37975484/1125159).

```
val df = Seq(
  ("bar", 2L),
  ("bar", 2L),
  ("foo", 1L),
  ("foo", 2L)
).toDF("word", "num")
```

The `cube` function "takes a list of columns and applies aggregate expressions to all possible combinations of the grouping columns".

```
df
  .cube($"word", $"num")
  .count()
  .sort(asc("word"), asc("num"))
  .show()
```

```
+----+----+-----+
|word| num|count|
+----+----+-----+
|null|null|    4| Total rows in df
|null|   1|    1| Count where num equals 1
|null|   2|    3| Count where num equals 2
| bar|null|    2| Where word equals bar
| bar|   2|    2| Where word equals bar and num equals 2
| foo|null|    2| Where word equals foo
| foo|   1|    1| Where word equals foo and num equals 1
| foo|   2|    1| Where word equals foo and num equals 2
+----+----+-----+
```

The order of the arguments passed to the `cube()` function don't matter, so `cube($"word", $"num")` will return the same results as `cube($"num", $"word")`.

## rollup()

`rollup` is a subset of `cube` that "computes hierarchical subtotals from left to right".

```
df
  .rollup($"word", $"num")
  .count()
  .sort(asc("word"), asc("num"))
  .show()
```

```
+----+----+-----+
|word| num|count|
+----+----+-----+
|null|null|    4| Count of all rows
| bar|null|    2| Count when word is bar
| bar|   2|    2| Count when num is 2
| foo|null|    2| Count when word is foo
| foo|   1|    1| When word is foo and num is 1
| foo|   2|    1| When word is foo and num is 2
+----+----+-----+
```

`rollup()` returns a subset of the rows returned by `cube()`. `rollup` returns 6 rows whereas `cube` returns 8 rows. Here are the missing rows.

```
+----+----+-----+
|word| num|count|
+----+----+-----+
|null|   1|    1| Word is null and num is 1
|null|   2|    3| Word is null and num is 2
+----+----+-----+
```

`rollup($"word", $"num")` doesn't return the counts when only `word` is `null`.

Let's switch around the order of the arguments passed to `rollup` and view the difference in the results.

```
df
  .rollup($"num", $"word")
  .count()
  .sort(asc("word"), asc("num"))
  .select("word", "num", "count")
  .show()
```

```
+----+----+-----+
|word| num|count|
+----+----+-----+
|null|null|    4|
|null|   1|    1|
|null|   2|    3|
| bar|   2|    2|
| foo|   1|    1|
| foo|   2|    1|
+----+----+-----+
```

Here are the rows missing from `rollup($"num", $"word")` compared to `cube($"word", $"num")`.

```
+----+----+-----+
|word| num|count|
+----+----+-----+
| bar|null|    2| Word equals bar and num is null
| foo|null|    2| Word equals foo and num is null
+----+----+-----+
```

`rollup($"num", $"word")` doesn't return the counts when only `num` is `null`.

## Next steps

Spark makes it easy to run aggregations at scale.

In production applications, you'll often want to do much more than run a simple aggregation. You'll want to verify the correctness of your code with tests and incrementally update aggregations. Make sure [you learn how to test your aggregation functions](https://leanpub.com/testing-spark/)!

If you're still struggling with the Spark basics, make sure to [read a good book to grasp the fundamentals](https://leanpub.com/beautiful-spark/).

Study the `groupBy` function, the [aggregate functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$), and the [RelationalGroupedDataset class](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset) to quickly master aggregations in Spark.
