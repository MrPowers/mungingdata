---
title: "Important Considerations when filtering in Spark with filter and where"
date: "2020-04-20"
categories: 
  - "apache-spark"
---

# Important Considerations when filtering in Spark with filter and where

This blog post explains how to filter in Spark and discusses the vital factors to consider when filtering.

Poorly executed filtering operations are a common bottleneck in Spark analyses.

You need to make sure your data is stored in a format that is efficient for Spark to query. You also need to make sure the number of memory partitions after filtering is appropriate for your dataset.

Executing a filtering query is easy… filtering well is difficult. Read the [Beautiful Spark book](https://leanpub.com/beautiful-spark) if you want to learn how to create create data lakes that are optimized for performant filtering operations.

Read this blog post closely. Filtering properly will make your analyses run faster and save your company money. It's the easiest way to become a better Spark programmer.

## Filter basics

Let's create a DataFrame and view the contents:

```scala
val df = Seq(
  ("famous amos", true),
  ("oreo", true),
  ("ginger snaps", false)
).toDF("cookie_type", "contains_chocolate")
```

```
df.show()

+------------+------------------+
| cookie_type|contains_chocolate|
+------------+------------------+
| famous amos|              true|
|        oreo|              true|
|ginger snaps|             false|
+------------+------------------+
```

Now let's filter the DataFrame to only include the rows with `contains_chocolate` equal to `true`.

```scala
val filteredDF = df.where(col("contains_chocolate") === lit(true))

filteredDF.show()

+-----------+------------------+
|cookie_type|contains_chocolate|
+-----------+------------------+
|famous amos|              true|
|       oreo|              true|
+-----------+------------------+
```

There are various alternate syntaxes that give you the same result and same performance.

- `df.where("contains_chocolate = true")`
- `df.where($"contains_chocolate" === true)`
- `df.where('contains_chocolate === true)`

A separate section towards the end of this blog post demonstrates that all of these syntaxes generate the same execution plan, so they'll all perform equally.

`where` is an alias for `filter`, so all these work as well:

- `df.filter(col("contains_chocolate") === lit(true))`
- `df.filter("contains_chocolate = true")`
- `df.filter($"contains_chocolate" === true)`
- `df.filter('contains_chocolate === true)`

## Empty partition problem

A filtering operation does not change the number of memory partitions in a DataFrame.

Suppose you have a data lake with 25 billion rows of data and 60,000 memory partitions. Suppose you run a filtering operation that results in a DataFrame with 10 million rows. After filtering, you'll still have 60,000 memory partitions, many of which will be empty. You'll need to run `repartition()` or `coalesce()` to spread the data on an appropriate number of memory partitions.

Let's look at some pseudocode:

```scala
val df = spark.read.parquet("/some/path") // 60,000 memory partitions
val filteredDF = df.filter(col("age") > 98) // still 60,000 memory partitions
// at this point, any operations performed on filteredDF will be super inefficient
val repartitionedDF = filtereDF.repartition(200) // down to 200 memory partitions
```

Let's use the `person_data.csv` file that contains 100 rows of data and `person_name` and `person_country` columns to demonstrate this on a real dataset.

80 people are from China, 15 people are from France, and 5 people are from Cuba.

This code reads in the `person_data.csv` file and [repartitions](https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4) the data into 200 memory partitions.

```scala
val path = new java.io.File("./src/test/resources/person_data.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .csv(path)
  .repartition(200)

println(df.rdd.partitions.size) // 200
```

Let's filter the DataFrame and verify that the number of memory partitions does not change:

```scala
val filteredDF = df.filter(col("person_country") === "Cuba")
println(filteredDF.rdd.partitions.size) // 200
```

There are only 5 rows of Cuba data and 200 memory partitions, so we know that at least 195 memory partitions are empty.

Having a lot of empty memory partitions significantly slows down analyses on production-sized datasets.

## Selecting an appropriate number of memory partitions

Choosing the right number of memory partitions after filtering is difficult.

You can follow the 1GB per memory partition rule of thumb to estimate the number of memory partitions that'll be appropriate for a filtered dataset.

Suppose you have 25 billion rows of data, which is 10 terabytes on disk (10,000 GB).

An extract with 500 million rows (2% of the total data) is probably around 200 GB of data (0.02 \* 10,000), so 200 memory partitions should work well.

## Underlying data stores

Filtering operations execute completely differently depending on the underlying data store.

Spark attempts to "push down" filtering operations to the database layer whenever possible because databases are optimized for filtering. This is called predicate pushdown filtering.

An operation like `df.filter(col("person_country") === "Cuba")` is executed differently depending on if the data store supports predicate pushdown filtering.

- A parquet lake will send all the data to the Spark cluster, and perform the filtering operation on the Spark cluster
- A Postgres database table will perform the filtering operation in Postgres, and then send the resulting data to the Spark cluster.

N.B. using a data lake that doesn't allow for query pushdown is a common, and potentially massive bottleneck.

## Column pruning

Spark will use the minimal number of columns possible to execute a query.

The `df.select("person_country").distinct()` query will be executed differently depending on the file format:

- A Postgres database will perform the filter at the database level and only send a subset of the `person_country` column to the cluster
- A Parquet data store will send the entire `person_country` column to the cluster and perform the filtering on the cluster (it doesn't send the `person_name` column - that column is "pruned")
- A CSV data store will send the entire dataset to the cluster. CSV is a row based file format and row based file formats don't support column pruning.

You almost always want to work with a file format or database that supports column pruning for your Spark analyses.

## Cluster sizing after filtering

Depending on the data store, the cluster size needs might be completely different before and after performing a filtering operation.

Let's say your 25 billion row dataset is stored in a parquet data lake and you need to perform a big filter and then do some advanced NLP on 1 million rows. You'll need a big cluster to perform the initial filtering operation and a smaller cluster to perform the NLP analysis on the comparatively tiny dataset. For workflows like these, it's often better to perform the filtering operation on a big cluster, repartition the data, write it to disk, and then perform the detailed analysis with a separate, smaller cluster on the extract.

Transferring big datasets from cloud storage to a cloud cluster and performing a big filtering operation is slow and expensive. You will generate a huge cloud compute bill with these types of workflows.

The pre / post filtering cluster requirements don't change when you're using a data storage that allows for query pushdown. The filtering operation is not performed in the Spark cluster. So you only need to use a cluster that can handle the size of the filtered dataset.

## Partition filters

Data lakes can be partitioned on disk with [partitionBy](https://mungingdata.com/apache-spark/partitionby/).

If the data lake is partitioned, [Spark can use PartitionFilters](https://mungingdata.com/apache-spark/partition-filters-pushed-filters/), as long as the filter is using the partition key.

In our example, we could make a partitioned data lake with the `person_country` partition key as follows:

```scala
val path = new java.io.File("./src/test/resources/person_data.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .csv(path)
  .repartition(col("person_country"))

df
  .write
  .partitionBy("person_country")
  .option("header", "true")
  .csv("tmp/person_data_partitioned")
```

This'll write out the data as follows:

```
person_data_partitioned/
  person_country=China/
    part-00059-dd8849eb-4e7d-4b6c-9536-59f94ea56412.c000.csv
  person_country=Cuba/
    part-00086-dd8849eb-4e7d-4b6c-9536-59f94ea56412.c000.csv
  person_country=France/
    part-00030-dd8849eb-4e7d-4b6c-9536-59f94ea56412.c000.csv
```

The "partition key" is `person_country`. Let's use `explain` to verify that PartitionFilters are used when filtering on the partition key.

```scala
val partitionedPath = new java.io.File("tmp/person_data_partitioned").getCanonicalPath
spark
  .read
  .csv(partitionedPath)
  .filter(col("person_country") === "Cuba")
  .explain()
```

```
FileScan csv [_c0#132,person_country#133]
  Batched: false,
  Format: CSV,
  Location: InMemoryFileIndex[file:/Users/matthewpowers/Documents/code/my_apps/mungingdata/spark2/tmp/person_...,
  PartitionCount: 1,
  PartitionFilters: [isnotnull(person_country#133), (person_country#133 = Cuba)],
  PushedFilters: [],
  ReadSchema: struct<_c0:string>
```

Check out [Beautiful Spark Code](https://leanpub.com/beautiful-spark) for a full description on how to build, update, and filter partitioned data lakes.

## Explain with different filter syntax

`filter` and `where` are executed the same, regardless of whether column arguments or SQL strings are used.

Let's verify that all the different filter syntaxes generate the same physical plan.

All of these code snippets generate the same physical plan:

```scala
df.where("person_country = 'Cuba'").explain()
df.where($"person_country" === "Cuba").explain()
df.where('person_country === "Cuba").explain()
df.filter("person_country = 'Cuba'").explain()
```

Here's the generated physical plan:

```
== Physical Plan ==
(1) Project [person_name#152, person_country#153]
+- (1) Filter (isnotnull(person_country#153) && (person_country#153 = Cuba))
   +- (1) FileScan csv [person_name#152,person_country#153]
          Batched: false,
      Format: CSV,
          Location: InMemoryFileIndex[file:/Users/matthewpowers/Documents/code/my_apps/mungingdata/spark2/src/test/re...,
          PartitionFilters: [],
          PushedFilters: [IsNotNull(person_country),
          EqualTo(person_country,Cuba)],
          ReadSchema: struct<person_name:string,person_country:string>
```

## Incremental updates with filter

Some filtering operations are easy to incrementally update with Structured Streaming + Trigger.Once.

See [this blog post](https://mungingdata.com/apache-spark/incrementally-updating-extracts/) for more details.

Incrementally updating a dataset is often 100 times faster than rerunning the query on the entire dataset.

## Conclusion

There are different syntaxes for filtering [Spark DataFrames](https://mungingdata.com/apache-spark/introduction-to-dataframes/) that are executed the same under the hood.

Optimizing filtering operations depends on the underlying data store. Your queries will be a lot more performant if the data store supports predicate pushdown filters.

If you're working with a data storage format that doesn't support predicate pushdown filters, try to create a partitioned data lake and leverages partition filters.

Transferring large datasets to the Spark cluster and performing the filtering in Spark is generally the slowest and most costly option. Avoid this query pattern whenever possible.

Filtering a Spark dataset is easy, but filtering in a performant, cost efficient manner is surprisingly hard. Filtering is a common bottleneck in Spark analyses.
