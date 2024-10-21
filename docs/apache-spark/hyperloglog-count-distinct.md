---
title: "Using HyperLogLog for count distinct computations with Spark"
date: "2019-11-04"
categories: 
  - "apache-spark"
---

# Using HyperLogLog for count distinct computations with Spark

This blog post explains how to use the HyperLogLog algorithm to perform fast count distinct operations.

HyperLogLog sketches can be generated with [spark-alchemy](https://github.com/swoop-inc/spark-alchemy/), loaded into Postgres databases, and queried with millisecond response times.

Let's start by exploring the built-in Spark approximate count functions and explain why it's not useful in most situations.

## Simple example with approx\_count\_distinct

Suppose we have the following `users1.csv` file:

```
user_id,first_name
1,bob
1,bob
2,cathy
2,cathy
3,ming
```

Let's use the `approx_count_distinct` function to estimate the unique number of distinct `user_id` values in the dataset.

```
val path1 = new java.io.File("./src/test/resources/users1.csv").getCanonicalPath

val df1 = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path1)

df1
  .agg(approx_count_distinct("user_id").as("approx_user_id_count"))
  .show()

+--------------------+
|approx_user_id_count|
+--------------------+
|                   3|
+--------------------+
```

`approx_count_distinct` uses the HyperLogLog algorithm under the hood and will return a result faster than a precise count of the distinct tokens (i.e. `df1.select("user_id").distinct().count()` will run slower).

`approx_count_distinct` is good for an ad-hoc query, but won't help you build a system with HyperLogLog sketches that can be queried with milisecond response times.

Let's review the count distinct problem at a high level and dive into an open source library that does allow for millisecond response times.

## Overview of count distinct problem

Precise distinct counts can not be reaggregated and updated incrementally.

If you have 5 unique visitors to a website and 3 unique visitors on day two, how many total visitors have you had to your site? Between 5 and 8.

Suppose the unique visitors at the end of day 1 is stored as an integer (5). On day 2, you won't be able to use the day 1 unique count when calculating the updated number of unique visitors. You'll need to rerun the entire count distinct query on the entire dataset.

HyperLogLog sketches are reaggregatable and can be incrementally updated.

The native Spark approx\_count\_distinct function does not expose the underlying HLL sketch, so it does allow users to build systems with incremental updates.

[Sim has a great talk on HyperLogLogs and reaggregation](https://databricks.com/session_eu19/high-performance-advanced-analytics-with-spark-alchemy) with more background information.

Let's turn to an open source library that exposes HLL sketches.

## Simple example with spark-alchemy

The open source [spark-alchemy](https://github.com/swoop-inc/spark-alchemy/) library makes it easy to create, merge, and calculate the number of distinct items in a HyperLogLog sketch.

Let's use the `hll_init` function to append a HyperLogLog sketch to each row of data in a DataFrame.

```
import com.swoop.alchemy.spark.expressions.hll.functions._

val path1 = new java.io.File("./src/test/resources/users1.csv").getCanonicalPath

val df1 = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path1)

df1
  .withColumn("user_id_hll", hll_init("user_id"))
  .show()

+-------+----------+--------------------+
|user_id|first_name|         user_id_hll|
+-------+----------+--------------------+
|      1|       bob|[FF FF FF FE 09 0...|
|      1|       bob|[FF FF FF FE 09 0...|
|      2|     cathy|[FF FF FF FE 09 0...|
|      2|     cathy|[FF FF FF FE 09 0...|
|      3|      ming|[FF FF FF FE 09 0...|
+-------+----------+--------------------+
```

Let's verify that the `user_id_hll` is a `BinaryType` column:

```
df1
  .withColumn("user_id_hll", hll_init("user_id"))
  .printSchema()

root
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- user_id_hll: binary (nullable = true)
```

Let's use the `hll_merge` function to merge all of the HLL sketckes into a single row of data:

```
df1
  .withColumn("user_id_hll", hll_init("user_id"))
  .select(hll_merge("user_id_hll").as("user_id_hll"))
  .show()

+--------------------+
|         user_id_hll|
+--------------------+
|[FF FF FF FE 09 0...|
+--------------------+
```

Write out the HyperLogLog sketch to disk and use the `hll_cardinality()` function to estimate the number of unique `user_id` values in the sketch.

```
val sketchPath1 = new java.io.File("./tmp/sketches/file1").getCanonicalPath

df1
  .withColumn("user_id_hll", hll_init("user_id"))
  .select(hll_merge("user_id_hll").as("user_id_hll"))
  .write
  .parquet(sketchPath1)

val sketch1 = spark.read.parquet(sketchPath1)

sketch1
  .select(hll_cardinality("user_id_hll"))
  .show()

+----------------------------+
|hll_cardinality(user_id_hll)|
+----------------------------+
|                           3|
+----------------------------+
```

Let's look at how we can incrementally update the HyperLogLog sketch and rerun the `hll_cardinality` computation.

## Incrementally updating HyperLogLog sketch

Suppose we have the following `users2.csv` file:

```
user_id,first_name
1,bob
1,bob
1,bob
1,bob
2,cathy
8,camilo
9,maria
```

Our new data file has two new `user_id` values (8 and 9) and two existing `user_id` values (`user_id` 1 and 2).

Let's build a HyperLogLog sketch with the new data, merge the new HLL sketch with the existing HyperLogLog sketch we wrote to disk, and rerun the `hll_cardinality` computation.

```
val path2 = new java.io.File("./src/test/resources/users2.csv").getCanonicalPath

val df2 = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path2)

df2
  .withColumn("user_id_hll", hll_init("user_id"))
  .select("user_id_hll")
  .union(sketch1)
  .select(hll_merge("user_id_hll").as("user_id_hll"))
  .select(hll_cardinality("user_id_hll"))
  .show()

+----------------------------+
|hll_cardinality(user_id_hll)|
+----------------------------+
|                           5|
+----------------------------+
```

We don't need to rebuild the HyperLogLog sketch for the `users1.csv` file - we can use the existing HLL sketch. HyperLogLogs are reaggregatable can be incrementally updated quickly.

## Building cohorts

Suppose we have the following data on some gamers:

```
user_id,favorite_game,age
sean,halo,36
powers,smash,34
cohen,smash,33
angel,pokemon,32
madison,portal,24
pete,mario_maker,8
nora,smash,7
```

The business would like a count of all the gamers that are adults.

```
val path = new java.io.File("./src/test/resources/gamers.csv").getCanonicalPath

val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val adults = df
  .where(col("age") >= 18)
  .select(hll_init_agg("user_id").as("user_id_hll"))

adults
  .select(hll_cardinality("user_id_hll"))
  .show()

+----------------------------+
|hll_cardinality(user_id_hll)|
+----------------------------+
|                           5|
+----------------------------+
```

This result makes sense: sean, powers, cohen, angel, and madison are all adults in our dataset.

The business would also like a count of all the gamers with a favorite game of smash.

```
val favoriteGameSmash = df
  .where(col("favorite_game") === "smash")
  .select(hll_init_agg("user_id").as("user_id_hll"))

favoriteGameSmash
  .select(hll_cardinality("user_id_hll"))
  .show()

+----------------------------+
|hll_cardinality(user_id_hll)|
+----------------------------+
|                           3|
+----------------------------+
```

powers, cohen, and nora like smash in our dataset.

The business would also like a count of all the gamers that are adults or have a smash as their favorite game (this is called a cohort).

```
adults
  .union(favoriteGameSmash)
  .select(hll_merge("user_id_hll").as("user_id_hll"))
  .select(hll_cardinality("user_id_hll"))
  .show()

+----------------------------+
|hll_cardinality(user_id_hll)|
+----------------------------+
|                           6|
+----------------------------+
```

We can easily union cohorts without reprocessing data.

Suppose the adults HLL sketch is stored in one row and the smash favorite game HLL sketch is stored in another row of data. We only need to process two rows of data to derive the count of adults or gamers that like smash.

In a real world application, you can generate a bunch of HLL sketches that are incrementally updated. You can union these HLL sketches as you wish with millisecond response times.

## Interoperability and web speed response times

You can use `hll_convert` to load the HLL sketches in a Postgres database so they can be queried rapidly.

We could create a database table with two columns:

```
| cohort_name  | hll_sketch         |
|--------------|--------------------|
| adults       | binary_data        |
| smash_lovers | binary_data        |
```

Generating counts is a simple matter of querying a few rows of data.

Querying fewer rows of data is how to make big data distinct counts fast.

## HyperLogLog sketches with groupBy

We can use the `hll_init_agg` function to compute the number of adult and non-adult smash fans.

```
val path = new java.io.File("./src/test/resources/gamers.csv").getCanonicalPath

val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)
  .withColumn("is_adult", col("age") >= 18)

val resDF = df
  .groupBy("is_adult", "favorite_game")
  .agg(hll_init_agg("user_id").as("user_id_hll"))


// number of adults that like smash
resDF
  .where(col("is_adult") && col("favorite_game") === "smash")
  .select(hll_cardinality("user_id_hll"))
  .show()

+----------------------------+
|hll_cardinality(user_id_hll)|
+----------------------------+
|                           2|
+----------------------------+

// number of children that like smash
resDF
  .where(!col("is_adult") && col("favorite_game") === "smash")
  .select(hll_cardinality("user_id_hll"))
  .show()

+----------------------------+
|hll_cardinality(user_id_hll)|
+----------------------------+
|                           1|
+----------------------------+
```

The `hll_init_agg` functions allows for data to be grouped and opens up new options for how distinct counts can be sliced and diced.

## Incremental updates with Delta lake

Let me know if you're interested in learning more about how Delta lake makes it easier to incrementally update HLL sketches and I'll populate this section.

## Conclusion

Distinct counts are expensive to compute and difficult incrementally update.

The HyperLogLog algorithm makes it easy to quickly compute distinct counts.

Businesses often want to build cohorts with web-speed response times.

You can use the [spark-alchemy](https://github.com/swoop-inc/spark-alchemy/) library to precompute HLL sketches, store the HLL sketches in a Postgres database, and return cohort counts with millisecond response times.
