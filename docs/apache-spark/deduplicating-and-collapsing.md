---
title: "Deduplicating and Collapsing Records in Spark DataFrames"
date: "2018-10-06"
categories: 
  - "apache-spark"
---

# Deduplicating and Collapsing Records in Spark DataFrames

This blog post explains how to filter duplicate records from Spark DataFrames with the `dropDuplicates()` and `killDuplicates()` methods. It also demonstrates how to collapse duplicate records into a single row with the `collect_list()` and `collect_set()` functions.

Make sure to read [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) for a detailed overview of how to deduplicate production datasets and for background information on the [ArrayType columns](https://mungingdata.com/apache-spark/arraytype-columns/) that are returned when DataFrames are collapsed.

## Deduplicating DataFrames

Let's create a DataFrame with `letter1`, `letter2`, and `number1` columns.

```scala
val df = Seq(
  ("a", "b", 1),
  ("a", "b", 2),
  ("a", "b", 3),
  ("z", "b", 4),
  ("a", "x", 5)
).toDF("letter1", "letter2", "number1")

df.show()
```

```
+-------+-------+-------+
|letter1|letter2|number1|
+-------+-------+-------+
|      a|      b|      1|
|      a|      b|      2|
|      a|      b|      3|
|      z|      b|      4|
|      a|      x|      5|
+-------+-------+-------+
```

Some rows in the `df` DataFrame have the same `letter1` and `letter2` values. Let's use the `Dataset#dropDuplicates()` method to remove duplicates from the DataFrame.

```
df.dropDuplicates("letter1", "letter2").show()
```

```
+-------+-------+-------+
|letter1|letter2|number1|
+-------+-------+-------+
|      a|      x|      5|
|      z|      b|      4|
|      a|      b|      1|
+-------+-------+-------+
```

The `dropDuplicates` method chooses one record from the duplicates and drops the rest. This is useful for simple use cases, but collapsing records is better for analyses that can't afford to lose any valuable data.

## Killing duplicates

We can use the [spark-daria](https://github.com/MrPowers/spark-daria/) `killDuplicates()` method to completely remove all duplicates from a DataFrame.

```scala
import com.github.mrpowers.spark.daria.sql.DataFrameExt._

df.killDuplicates("letter1", "letter2").show()
```

```
+-------+-------+-------+
|letter1|letter2|number1|
+-------+-------+-------+
|      a|      x|      5|
|      z|      b|      4|
+-------+-------+-------+
```

Killing duplicates is similar to dropping duplicates, just a little more aggressive.

## Collapsing records

Let's use the `collect_list()` method to eliminate all the rows with duplicate `letter1` and `letter2` rows in the DataFrame and collect all the `number1` entries as a list.

```scala
df
  .groupBy("letter1", "letter2")
  .agg(collect_list("number1") as "number1s")
  .show()
```

```
+-------+-------+---------+
|letter1|letter2| number1s|
+-------+-------+---------+
|      a|      x|      [5]|
|      z|      b|      [4]|
|      a|      b|[1, 2, 3]|
+-------+-------+---------+
```

Let's create a more realitic example of credit card transactions and use `collect_set()` to aggregate unique records and eliminate pure duplicates.

```scala
val ccTransactionsDF = Seq(
  ("123", "20180102", 10.49),
  ("123", "20180102", 10.49),
  ("123", "20180102", 77.33),
  ("555", "20180214", 99.99),
  ("888", "20180214", 1.23)
).toDF("person_id", "transaction_date", "amount")

ccTransactionsDF.show()
```

```
+---------+----------------+------+
|person_id|transaction_date|amount|
+---------+----------------+------+
|      123|        20180102| 10.49|
|      123|        20180102| 10.49|
|      123|        20180102| 77.33|
|      555|        20180214| 99.99|
|      888|        20180214|  1.23|
+---------+----------------+------+
```

Let's eliminate the duplicates with `collect_set()`.

```scala
ccTransactionsDF
  .groupBy("person_id", "transaction_date")
  .agg(collect_set("amount") as "amounts")
  .show()
```

```
+---------+----------------+--------------+
|person_id|transaction_date|       amounts|
+---------+----------------+--------------+
|      555|        20180214|       [99.99]|
|      888|        20180214|        [1.23]|
|      123|        20180102|[10.49, 77.33]|
+---------+----------------+--------------+
```

`collect_set()` let's us retain all the valuable information and delete the duplicates. The best of both worlds!

## Collapsing records to datamarts

Let's examine a DataFrame of with data on hockey players and how many goals they've scored in each game.

```scala
val playersDF = Seq(
  ("123", 11, "20180102", 0),
  ("123", 11, "20180102", 0),
  ("123", 13, "20180105", 3),
  ("555", 11, "20180214", 1),
  ("888", 22, "20180214", 2)
).toDF("player_id", "game_id", "game_date", "goals_scored")

playersDF.show()
```

```
+---------+-------+---------+------------+
|player_id|game_id|game_date|goals_scored|
+---------+-------+---------+------------+
|      123|     11| 20180102|           0|
|      123|     11| 20180102|           0|
|      123|     13| 20180105|           3|
|      555|     11| 20180214|           1|
|      888|     22| 20180214|           2|
+---------+-------+---------+------------+
```

Let's create a `StructType` column that encapsulates all the columns in the DataFrame and then collapse all records on the `player_id` column to create a player datamart.

```scala
playersDF
  .withColumn("as_struct", struct("game_id", "game_date", "goals_scored"))
  .groupBy("player_id")
  .agg(collect_set("as_struct") as "as_structs")
  .show(false)
```

```
+---------+----------------------------------+
|player_id|as_structs                        |
+---------+----------------------------------+
|888      |[[22,20180214,2]]                 |
|555      |[[11,20180214,1]]                 |
|123      |[[11,20180102,0], [13,20180105,3]]|
+---------+----------------------------------+
```

A player datamart like this can simplify a lot of queries. We don't need to write window functions if all the data is already aggregated in a single row.

## Next steps

Deduplicating DataFrames is relatively straightforward. Collapsing records is more complicated, but worth the effort.

Data lakes are notoriously granular and programmers often write window functions to analyze historical results.

Collapsing records into datamarts is the best way to simplify your code logic.
