---
title: "Reverse Engineering Spark Structured Streaming and Trigger.Once"
date: "2019-08-11"
categories: 
  - "apache-spark"
---

# Reverse Engineering Spark Structured Streaming and Trigger.Once

Spark Structured Streaming and Trigger.Once make it easy to run incremental updates. Spark uses a checkpoint directory to identify the data that's already been processed and only analyzes the new data.

This blog post demonstrates how to use Structured Streaming and Trigger.Once and provides a detailed look at the checkpoint directory that easily allows Spark to identify the newly added files.

Let's start by running a simple example and examining the contents of the checkpoint directory.

## Simple example

Let's create a `dog_data_csv` directory with the following `dogs1` file to start.

```
first_name,breed
fido,lab
spot,bulldog
```

Let's use Spark Structured Streaming and Trigger.Once to write our all the CSV data in `dog_data_csv` to a `dog_data_parquet` data lake.

```scala
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

val csvPath = new java.io.File("./tmp/dog_data_csv/").getCanonicalPath

val schema = StructType(
  List(
    StructField("first_name", StringType, true),
    StructField("breed", StringType, true)
  )
)

val df = spark.readStream
  .schema(schema)
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(csvPath)

val checkpointPath = new java.io.File("./tmp/dog_data_checkpoint/").getCanonicalPath
val parquetPath = new java.io.File("./tmp/dog_data_parquet/").getCanonicalPath

df
  .writeStream
  .trigger(Trigger.Once)
  .format("parquet")
  .option("checkpointLocation", checkpointPath)
  .start(parquetPath)
```

The parquet data is written out in the `dog_data_parquet` directory. Let's print out the Parquet data to verify it only contains the two rows of data from our CSV file.

```scala
val parquetPath = new java.io.File("./tmp/dog_data_parquet/").getCanonicalPath
spark.read.parquet(parquetPath).show()

+----------+-------+
|first_name|  breed|
+----------+-------+
|      fido|    lab|
|      spot|bulldog|
+----------+-------+
```

The `dog_data_checkpoint` directory contains the following files.

```
dog_data_checkpoint/
  commits/
    0
  offsets/
    0
  sources/
    0/
      0
  metadata
```

Spark creates lots of JSON files in the checkpoint directory (the files don't have extensions for some reason). Let's take a peek at the metadata included in these files.

Here's the `dog_data_checkpoint/commits/0` file:

```
v1
{"nextBatchWatermarkMs":0}
```

Here's the `dog_data_checkpoint/offsets/0` file:

```
v1
{
  "batchWatermarkMs":0,
  "batchTimestampMs":1565368744601,
  "conf":{
    "spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2",
    "spark.sql.streaming.multipleWatermarkPolicy":"min",
    "spark.sql.streaming.aggregation.stateFormatVersion":"2",
    "spark.sql.shuffle.partitions":"200"
  }
}
{"logOffset":0}
```

Here's the `dog_data_checkpoint/sources/0/0` file:

```
v1
{
  "path":"file:///Users/powers/Documents/code/my_apps/yello-taxi/tmp/dog_data_csv/dogs1.csv",
  "timestamp":1565354770000,
  "batchId":0
}
```

## Incremental update

Let's add the `dogs2.csv` file to `tmp/dog_data_csv`, run the incremental update code, verify that the Parquet lake is incrementally updated, and explore what files are added to the checkpoint directory when an incremental update is run.

Here's the `tmp/dog_data_csv/dogs2.csv` file that'll be created:

```
first_name,breed
fido,beagle
lou,pug
```

The Trigger.Once code was wrapped in a method (see [this file](https://github.com/MrPowers/yellow-taxi/blob/master/src/main/scala/mrpowers/yellow/taxi/IncrementalDogUpdater.scala)) and can be run from the SBT console with this command: `mrpowers.yellow.taxi.IncrementalDogUpdater.update()`.

Let's verify the Parquet data lake has been updated with the new data:

```scala
val parquetPath = new java.io.File("./tmp/dog_data_parquet/").getCanonicalPath
spark.read.parquet(parquetPath).show()

+----------+-------+
|first_name|  breed|
+----------+-------+
|      fido|    lab|
|      spot|bulldog|
|      fido| beagle|
|       lou|    pug|
+----------+-------+
```

The `dog_data_checkpoint` directory now contains the following files.

```
dog_data_checkpoint/
  commits/
    0
    1
  offsets/
    0
    1
  sources/
    0/
      0
      1
  metadata
```

Here are the contents of the `dog_data_checkpoint/commits/1` file:

```
v1
{"nextBatchWatermarkMs":0}
```

Here's the `dog_data_checkpoint/offsets/1` file:

```
v1
{
  "batchWatermarkMs":0,
  "batchTimestampMs":1565446320529,
  "conf":{
    "spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2",
    "spark.sql.streaming.multipleWatermarkPolicy":"min",
    "spark.sql.streaming.aggregation.stateFormatVersion":"2",
    "spark.sql.shuffle.partitions":"200"
  }
}
{"logOffset":0}
```

Here's the `dog_data_checkpoint/sources/0/1` file:

```
v1
{
  "path":"file:///Users/powers/Documents/code/my_apps/yello-taxi/tmp/dog_data_csv/dogs2.csv",
  "timestamp":1565354885000,
  "batchId":1
}
```

## Incrementally updating aggregations manually

Let's start over and create a job that creates a running count of each dog by name. We don't want to reprocess files that have already been aggregated. We'd like to combine the existing results with the new files that are added, so we don't need to recalculate the same data.

We'll run the following code to create an initial cache of the counts by name.

```scala
val csvPath = new java.io.File("./tmp/dog_data_csv/dogs1.csv").getCanonicalPath
val df = spark.read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(csvPath)

val outputPath = new java.io.File("./tmp/dog_data_name_counts").getCanonicalPath
df
  .groupBy("first_name")
  .count()
  .repartition(1)
  .write
  .parquet(outputPath)
```

Let's inspect the aggregation after processing the first file.

```scala
val parquetPath = new java.io.File("./tmp/dog_data_name_counts/").getCanonicalPath
spark.read.parquet(parquetPath).show()

+----------+-----+
|first_name|count|
+----------+-----+
|      fido|    1|
|      spot|    1|
+----------+-----+
```

Let's run some code that will combine the aggregations from the first data file with the second data file, rerun the aggregations, and then write out the results. We will not aggregate the results in the first file again - we'll just combine our existing results with the second data file.

```scala
val csvPath = new java.io.File("./tmp/dog_data_csv/dogs2.csv").getCanonicalPath
val df = spark.read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(csvPath)

val existingCountsPath = new java.io.File("./tmp/dog_data_name_counts").getCanonicalPath
val tmpPath = new java.io.File("./tmp/dog_data_name_counts_tmp").getCanonicalPath
val existingCountsDF = spark
  .read
  .parquet(existingCountsPath)

df
  .union(existingCountsDF)
  .cache()
  .groupBy("first_name")
  .count()
  .repartition(1)
  .write
  .mode(SaveMode.Overwrite)
  .parquet(tmpPath)

spark
  .read
  .parquet(tmpPath)
  .write
  .mode(SaveMode.Overwrite)
  .parquet(existingCountsPath)
```

Let's inspect the aggregation after processing the incremental update.

```scala
val parquetPath = new java.io.File("./tmp/dog_data_name_counts/").getCanonicalPath
spark.read.parquet(parquetPath).show()

+----------+-----+
|first_name|count|
+----------+-----+
|      fido|    2|
|      spot|    1|
|       lou|    1|
+----------+-----+
```

This implementation isn't ideal because the initial run and incremental update require different code. Let's see if we can write a single method that'll be useful for the initial run and the incremental update.

## Refactored updating aggregations

Let's start out with a little helper method to check if a directory exists.

```scala
def dirExists(hdfsDirectory: String): Boolean = {
  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
}
```

Now we can write a method that's flexible enough to create or incrementally update the aggregate name counts, depending on if an output file already exists.

```scala
def refactoredUpdateCountByName(csvPath: String): Unit = {
  val df = spark.read
    .option("header", "true")
    .option("charset", "UTF8")
    .csv(csvPath)

  val existingCountsPath = new java.io.File("./tmp/dog_data_name_counts").getCanonicalPath
  val tmpPath = new java.io.File("./tmp/dog_data_name_counts_tmp").getCanonicalPath

  val unionedDF = if(dirExists(existingCountsPath)) {
    spark
      .read
      .parquet(existingCountsPath)
      .union(df)
  } else {
    df
  }

  unionedDF
    .groupBy("first_name")
    .count()
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(tmpPath)

  spark
    .read
    .parquet(tmpPath)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(existingCountsPath)
}
```

Let's run this method with both CSV files and examine the output.

```scala
val csvPath1 = new java.io.File("./tmp/dog_data_csv/dogs1.csv").getCanonicalPath
refactoredUpdateCountByName(csvPath1)
showDogDataNameCounts()

+----------+-----+
|first_name|count|
+----------+-----+
|      fido|    1|
|      spot|    1|
+----------+-----+

val csvPath2 = new java.io.File("./tmp/dog_data_csv/dogs2.csv").getCanonicalPath
refactoredUpdateCountByName(csvPath2)
showDogDataNameCounts()

+----------+-----+
|first_name|count|
+----------+-----+
|      fido|    2|
|      spot|    1|
|       lou|    1|
+----------+-----+
```

## Incrementally updating aggregates with Structured Streaming + Trigger.Once

It turns out that we can't build an incrementally updating aggregate file with Structured Streaming and Trigger.Once.

Structured Streaming and Trigger.Once allows for a lot of complicated features, many of which are not needed for batch analyses.

We need to build another open source solution that provides Spark developers with a flexible interface to run batch analysis. We can borrow the good ideas from Structured Streaming + Trigger.Once and make something that's a lot simpler, as we won't need to also consider streaming use cases.

## Proposed interface for the batch incremental updater

The batch incremental updater will track files that have already been processed and allow users to easily identify new files. Suppose a data lake contains 50,000 files that have already been processed and two new files are added. The batch incremental updater will make it easy for the user to identify the two newly added files for analysis.

The batch incremental updater will use the standard Dataset API (not the streaming API), so developers can easily access all batch features. For example, users can use `SaveMode.Overwrite` that's not supported by the streaming API.

## Next steps

Structured Streaming + Trigger.Once is great for simple batch updates.

We need a better solution for more complicated batch updates. This is a common use case and the community needs a great solution. Can't wait to collaborate with smart Spark engineers to get this built!
