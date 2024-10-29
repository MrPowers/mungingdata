---
title: "Fast Filtering with Spark PartitionFilters and PushedFilters"
date: "2019-07-23"
categories: 
  - "apache-spark"
---

# Fast Filtering with Spark PartitionFilters and PushedFilters

Spark can use the disk partitioning of files to greatly speed up certain filtering operations.

This post explains the difference between memory and disk partitioning, describes how to analyze physical plans to see when filters are applied, and gives a conceptual overview of why this design pattern can provide massive performace gains.

## Normal DataFrame filter

Let's create a CSV file (`/Users/powers/Documents/tmp/blog_data/people.csv`) with the following data:

```
first_name,last_name,country
Ernesto,Guevara,Argentina
Vladimir,Putin,Russia
Maria,Sharapova,Russia
Bruce,Lee,China
Jack,Ma,China
```

Let's read in the CSV data into a DataFrame:

```scala
val df = spark
  .read
  .option("header", "true")
  .csv("/Users/powers/Documents/tmp/blog_data/people.csv")
```

Let's write a query to fetch all the Russians in the CSV file with a `first_name` that starts with `M`.

```scala
df
  .where($"country" === "Russia" && $"first_name".startsWith("M"))
  .show()
```

```
+----------+---------+-------+
|first_name|last_name|country|
+----------+---------+-------+
|     Maria|Sharapova| Russia|
+----------+---------+-------+
```

Let's use `explain()` to see how the query is executed.

```scala
df
  .where($"country" === "Russia" && $"first_name".startsWith("M"))
  .explain()
```

```
== Physical Plan ==
Project [first_name#12, last_name#13, country#14]
+- Filter (((isnotnull(country#14) && isnotnull(first_name#12)) && (country#14 = Russia)) && StartsWith(first_name#12, M))
   +- FileScan csv [first_name#12,last_name#13,country#14]
        Batched: false,
        Format: CSV,
        Location: InMemoryFileIndex[file:/Users/powers/Documents/tmp/blog_data/people.csv],
        PartitionFilters: [],
        PushedFilters: [IsNotNull(country), IsNotNull(first_name), EqualTo(country,Russia), StringStartsWith(first_name,M)],
        ReadSchema: struct

```

Take note that there are no `PartitionFilters` in the physical plan.

## `partitionBy()`

The `repartition()` method partitions the data in memory and the `partitionBy()` method partitions data in folders when it's written out to disk.

Let's write out the data in partitioned CSV files.

```scala
df
  .repartition($"country")
  .write
  .option("header", "true")
  .partitionBy("country")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")
```

Here's what the directory structure looks like:

```
partitioned_lake/
  country=Argentina/
    part-00044-c5d2f540-e89b-40c1-869d-f9871b48c617.c000.csv
  country=China/
    part-00059-c5d2f540-e89b-40c1-869d-f9871b48c617.c000.csv
  country=Russia/
    part-00002-c5d2f540-e89b-40c1-869d-f9871b48c617.c000.csv
```

Here are the contents of the CSV file in the `country=Russia` directory.

```
first_name,last_name
Vladimir,Putin
Maria,Sharapova
```

Notice that the `country` column is not included in the CSV file anymore. Spark has abstracted a column from the CSV file to the directory name.

## PartitionFilters

Let's read from the partitioned data folder, run the same filters, and see how the physical plan changes.

Let's run the same filter as before, but on the partitioned lake, and examine the physical plan.

```scala
val partitionedDF = spark
  .read
  .option("header", "true")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")

partitionedDF
  .where($"country" === "Russia" && $"first_name".startsWith("M"))
  .explain()
```

```
== Physical Plan ==
Project [first_name#74, last_name#75, country#76]
+- Filter (isnotnull(first_name#74) && StartsWith(first_name#74, M))
   +- FileScan csv [first_name#74,last_name#75,country#76]
        Batched: false,
        Format: CSV,
        Location: InMemoryFileIndex[file:/Users/powers/Documents/tmp/blog_data/partitioned_lake],
        PartitionCount: 1,
        PartitionFilters: [isnotnull(country#76), (country#76 = Russia)],
        PushedFilters: [IsNotNull(first_name), StringStartsWith(first_name,M)],
        ReadSchema: struct

```

You need to examine the physical plans carefully to identify the differences.

When filtering on `df` we have `PartitionFilters: []` whereas when filtering on `partitionedDF` we have `PartitionFilters: [isnotnull(country#76), (country#76 = Russia)]`.

Spark only grabs data from certain partitions and skips all of the irrelevant partitions. Data skipping allows for a big performance boost.

## PushedFilters

When we filter off of `df`, the pushed filters are `[IsNotNull(country), IsNotNull(first_name), EqualTo(country,Russia), StringStartsWith(first_name,M)]`.

When we filter off of `partitionedDf`, the pushed filters are `[IsNotNull(first_name), StringStartsWith(first_name,M)]`.

Spark doesn't need to push the `country` filter when working off of `partitionedDF` because it can use a partition filter that is a lot faster.

## Partitioning in memory vs. partitioning on disk

`repartition()` and `coalesce()` change how data is partitioned in memory.

`partitionBy()` changes how data is partitioned when it's written out to disk.

Use `repartition()` before writing out partitioned data to disk with `partitionBy()` because it'll execute a lot faster and write out fewer files.

Partitioning in memory and paritioning on disk are related, but completely different concepts that expert Spark programmers must master.

## Disk partitioning with skewed columns

Suppose you have a data lake with information on all 7.6 billion people in the world. The country column is skewed because a lot of people live in countries like China and India and compatively few people live in countries like Montenegro.

This code is problematic because it will write out the data in each partition as a single file.

```scala
df
  .repartition($"country")
  .write
  .option("header", "true")
  .partitionBy("country")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")
```

We don't our data lake to contain some massive files because that'll make Spark reads / writes unnecessarily slow.

If we don't do any in memory reparitioning, Spark will write out a ton of files for each partition and our data lake will contain way too many small files.

```scala
df
  .write
  .option("header", "true")
  .partitionBy("country")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")
```

[This answer](https://stackoverflow.com/questions/53037124/partitioning-a-large-skewed-dataset-in-s3-with-sparks-partitionby-method) explains how to intelligently repartition in memory before writing out to disk with `partitionBy()`.

Here's how we can limit each partition to a maximum of 100 files.

```scala
import org.apache.spark.sql.functions.rand

df
  .repartition(100, $"country", rand)
  .write
  .option("header", "true")
  .partitionBy("country")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")
```

## Next steps

I recommend rereading this blog post and running all the code on your local machine with the [Spark shell](https://www.mungingdata.com/apache-spark/using-the-console).

Effective disk partitioning can greatly speed up filter operations.
