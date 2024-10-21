---
title: "AWS Athena and Apache Spark are Best Friends"
date: "2018-12-10"
categories: 
  - "aws"
---

# AWS Athena and Apache Spark are Best Friends

Apache Spark makes it easy to build data lakes that are optimized for AWS Athena queries.

This blog post will demonstrate that it's easy to follow the AWS Athena tuning tips with a tiny bit of Spark code - let's dive in!

## Creating Parquet Data Lake

We can convert a CSV data lake to a Parquet data lake with AWS Glue or we can write a couple lines of Spark code.

```
val df = spark.read.csv("/mnt/my-bucket/csv-lake/")
spark.write.parquet("/mnt/my-bucket/parquet-lake/")
```

The pure Spark solution is less complicated than the AWS Glue solution if your company already has an environment setup to run Spark code (like Databricks). AWS Glue uses Spark under the hood, so they're both Spark solutions at the end of the day.

## Incrementally updating Parquet lake

Suppose your CSV data lake is incrementally updated and you'd also like to incrementally update your Parquet data lake for Athena queries.

Spark allows for incremental updates with Structured Streaming and Trigger.Once.

Here's example code for an incremental update job.

```
import org.apache.spark.sql.streaming.Trigger

val sDF = spark.readStream.format("csv").load("/mnt/my-bucket/csv-lake/")

sDF
  .writeStream
  .trigger(Trigger.Once)
  .format("parquet")
  .option("checkpointLocation", "/mnt/my-bucket/incremental-parquet-lake-checkpoint/")
  .start("/mnt/my-bucket/incremental-parquet-lake/")
```

The checkpoint directory tracks the files that have already been loaded into the incremental Parquet data lake. Spark grabs the new CSV files and loads them into the Parquet data lake every time the job is run.

## Creating a hive partitioned lake

The #1 AWS Athena tuning tip is to partition your data.

A partitioned data set limits the amount of data that Athena needs to scan for certain queries.

The Spark `partitionBy` method makes it easy to partition data in disc with directory naming conventions that work with Athena (the standard Hive partition naming conventions).

Read [this blog post for more background on partitioning with Spark](https://www.mungingdata.com/apache-spark/partition-filters-pushed-filters).

Let's write some code that converts a standard Parquet data lake into a Parquet data lake that's partitioned in disc on the `country` column.

```
val df = spark.read.parquet("/mnt/my-bucket/parquet-lake/")

df
  .write
  .partitionBy("country")
  .parquet("/mnt/my-bucket/partitioned_lake")
```

Here's how to create a partitioned table in Athena.

```
CREATE EXTERNAL TABLE IF NOT EXISTS this_is_awesome(
  first_name STRING,
  last_name STRING
)
PARTITIONED BY (country STRING)
STORED AS PARQUET
LOCATION 's3://my-bucket/partitioned_lake'
```

The partitioned table will make queries like this run faster:

```
select count(*) from this_is_awesome where country = 'Malaysia'
```

This blog post discusses how Athena works with partitioned data sources in more detail.

## Programmatically creating Athena tables

It can be really annoying to create AWS Athena tables for Spark data lakes, especially if there are a lot of columns. Athena should really be able to infer the schema from the Parquet metadata, but that’s another rant.

The [spark-daria](https://github.com/MrPowers/spark-daria) `printAthenaCreateTable()` method makes this easier by programmatically generating the Athena `CREATE TABLE` code from a Spark DataFrame.

Suppose we have this DataFrame (`df`):

```
+--------+--------+---------+
|    team|   sport|goals_for|
+--------+--------+---------+
|    jets|football|       45|
|nacional|  soccer|       10|
+--------+--------+---------+
```

Run this [spark-daria](https://github.com/MrPowers/spark-daria) code to generate the Athena `CREATE TABLE` query.

```
import com.github.mrpowers.spark.daria.sql.DataFrameHelpers

DataFrameHelpers.printAthenaCreateTable(
  df,
  "my_cool_athena_table",
  "s3://my-bucket/extracts/people"
)
```

```
CREATE TABLE IF NOT EXISTS my_cool_athena_table(
  team STRING,
  sport STRING,
  goals_for INT
)
STORED AS PARQUET
LOCATION 's3://my-bucket/extracts/people'
```

You'll thank me for this helper method if you ever have to create a table with 100+ columns :wink:

## Conclusion

It's easy to build data lakes that are optimized for AWS Athena queries with Spark.

Spinning up a Spark cluster to run simple queries can be overkill. Athena is great for quick queries to explore a Parquet data lake.

Athena and Spark are best friends - have fun using them both!
