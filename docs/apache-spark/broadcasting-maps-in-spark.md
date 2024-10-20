---
title: "Broadcasting Maps in Spark"
date: "2019-08-28"
categories: 
  - "apache-spark"
---

Spark makes it easy to broadcast maps and perform hash lookups in a cluster computing environment.

This post explains how to broadcast maps and how to use these broadcasted variables in analyses.

## Simple example

Suppose you have an ArrayType column with a bunch of first names. You'd like to use a nickname map to standardize all of the first names.

Here's how we'd write this code for a single Scala array.

```
import scala.util.Try

val firstNames = Array("Matt", "Fred", "Nick")
val nicknames = Map("Matt" -> "Matthew", "Nick" -> "Nicholas")
val res = firstNames.map { (n: String) =>
  Try { nicknames(n) }.getOrElse(n)
}
res // equals Array("Matthew", "Fred", "Nicholas")
```

Let's create a DataFrame with an ArrayType column that contains a list of first names and then append a `standardized_names` column that runs all the names through a Map.

```
import scala.util.Try

val nicknames = Map("Matt" -> "Matthew", "Nick" -> "Nicholas")
val n = spark.sparkContext.broadcast(nicknames)

val df = spark.createDF(
  List(
    (Array("Matt", "John")),
    (Array("Fred", "Nick")),
    (null)
  ), List(
    ("names", ArrayType(StringType, true), true)
  )
).withColumn(
  "standardized_names",
  array_map((name: String) => Try { n.value(name) }.getOrElse(name))
    .apply(col("names"))
)

df.show(false)

+------------+------------------+
|names       |standardized_names|
+------------+------------------+
|[Matt, John]|[Matthew, John]   |
|[Fred, Nick]|[Fred, Nicholas]  |
|null        |null              |
+------------+------------------+
```

We use the `spark.sparkContext.broadcast()` method to broadcast the `nicknames` map to all nodes in the cluster.

Spark 2.4 added a `transform` method that's similar to the Scala `Array.map()` method, but this isn't easily accessible via the Scala API yet, so we map through all the array elements with the [spark-daria](https://github.com/MrPowers/spark-daria/) `array_map` method.

Note that we need to call `n.value()` to access the broadcasted value. This is slightly different than what's needed when writing vanilla Scala code.

We have some code that works which is a great start. Let's clean this code up with some good Spark coding practices.

## Refactored code

Let's wrap the `withColumn` code in a [Spark custom transformation](https://medium.com/@mrpowers/chaining-custom-dataframe-transformations-in-spark-a39e315f903c), so it's more modular and easier to test.

```
val nicknames = Map("Matt" -> "Matthew", "Nick" -> "Nicholas")
val n = spark.sparkContext.broadcast(nicknames)

def withStandardizedNames(n: org.apache.spark.broadcast.Broadcast[Map[String, String]])(df: DataFrame) = {
  df.withColumn(
    "standardized_names",
    array_map((name: String) => Try { n.value(name) }.getOrElse(name))
      .apply(col("names"))
  )
}

val df = spark.createDF(
  List(
    (Array("Matt", "John")),
    (Array("Fred", "Nick")),
    (null)
  ), List(
    ("names", ArrayType(StringType, true), true)
  )
).transform(withStandardizedNames(n))

df.show(false)

+------------+------------------+
|names       |standardized_names|
+------------+------------------+
|[Matt, John]|[Matthew, John]   |
|[Fred, Nick]|[Fred, Nicholas]  |
|null        |null              |
+------------+------------------+
```

The `withStandardizedNames()` transformation takes a `org.apache.spark.broadcast.Broadcast[Map[String, String]])` as an argument. We can pass our broadcasted Map around as a argument to functions. Scala is awesome.

## Building Maps from data files

You can store the nickname data in a CSV file, convert it to a Map, and then broadcast the Map. It's typically best to store data in a CSV file instead of in a Map that lives in your codebase.

Let's create a little CSV file with our nickname to firstname mappings.

```
nickname,firstname
Matt,Matthew
Nick,Nicholas
```

Let's read the nickname CSV into a DataFrame, convert it to a Map, and then broadcast it.

```
import com.github.mrpowers.spark.daria.sql.DataFrameHelpers

val nicknamesPath = new java.io.File(s"./src/test/resources/nicknames.csv").getCanonicalPath

val nicknamesDF = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(nicknamesPath)

val nicknames = DataFrameHelpers.twoColumnsToMap[String, String](
  nicknamesDF,
  "nickname",
  "firstname"
)

val n = spark.sparkContext.broadcast(nicknames)

def withStandardizedNames(n: org.apache.spark.broadcast.Broadcast[Map[String, String]])(df: DataFrame) = {
  df.withColumn(
    "standardized_names",
    array_map((name: String) => Try { n.value(name) }.getOrElse(name))
      .apply(col("names"))
  )
}

val df = spark.createDF(
  List(
    (Array("Matt", "John")),
    (Array("Fred", "Nick")),
    (null)
  ), List(
    ("names", ArrayType(StringType, true), true)
  )
).transform(withStandardizedNames(n))

df.show(false)

+------------+------------------+
|names       |standardized_names|
+------------+------------------+
|[Matt, John]|[Matthew, John]   |
|[Fred, Nick]|[Fred, Nicholas]  |
|null        |null              |
+------------+------------------+
```

This code uses the [spark-daria](https://github.com/MrPowers/spark-daria) `DataFrameHelpers.twoColumnsToMap()` method to convert the DataFrame to a Map. Use `spark-daria` whenever possible for these utility-type operations, so you don't need to reinvent the wheel.

## Conclusion

You'll often want to broadcast small Spark DataFrames when making [broadcast joins](https://mungingdata.com/apache-spark/broadcast-joins/).

This post illustrates how broadcasting Spark Maps is a powerful design pattern when writing code that executes on a cluster.

Feel free to broadcast any variable to all the nodes in the cluster. You'll get huge performance gains whenever code is run in parallel on various nodes.
