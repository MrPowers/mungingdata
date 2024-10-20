---
title: "Compacting Small Files in Delta Lakes"
date: "2019-09-05"
categories: 
  - "delta-lake"
---

This post explains how to compact small files in Delta lakes with Spark.

Data lakes can accumulate a lot of small files, especially when they're incrementally updated. Small files cause read operations to be slow.

Joining small files into bigger files via compaction is an important data lake maintenance technique to keep reads fast.

## Simple example

Let's create a Delta data lake with 1,000 files and then compact the folder to only contain 10 files.

Here's the code to create the Delta lake with 1,000 files:

```
df
  .repartition(1000)
  .write
  .format("delta")
  .save("/some/path/data")
```

The `_delta_log/00000000000000000000.json` file will contain 1,000 rows like this:

```
{
  "add":{
    "path":"part-00000-some-stuff-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":12345,
    "modificationTime":1567528151000,
    "dataChange":true
  }
}
```

Let's compact the data to only contain 10 files.

```
val df = spark
  .read
  .format("delta")
  .load("/some/path/data")

df
  .repartition(10)
  .write
  .format("delta")
  .mode("overwrite")
  .save("/some/path/data")
```

The `/some/path/data` now contains 1,010 files - the 1,000 original uncompacted files and the 10 compacted files.

The `_delta_log/00000000000000000001.json` file will contain 10 rows like this:

```
{
  "add":{
    "path":"part-00000-compacted-data-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":123456,
    "modificationTime":1567528453000,
    "dataChange":true
  }
}
```

The `_delta_log/00000000000000000001.json` file will also contain 1,000 rows like this:

```
{
  "remove":{
    "path":"part-00154-some-stuff-c000.snappy.parquet",
    "deletionTimestamp":1567528580934,
    "dataChange":true
  }
}
```

You can run the `vacuum()` command to delete the old data files, so you don't have to pay to store the uncompacted data.

## dataChange=false

[From Michael Armbrust](https://github.com/delta-io/delta/issues/146):

> The Delta transaction protocol contains the ability to mark entries in the transaction log as dataChange=false indicating that they are only rearranging data that is already part of the table. This is powerful as it allows you to perform compaction and other read-performance optimizations, without breaking the ability to use a Delta table as a streaming source. We should expose this as a DataFrame writer option for overwrites.

Delta lake currently sets `dataChange=true` when data is compacted, which is a breaking change for downstream streaming consumers. Delta lake will be updated to give users the option to set `dataChange=false` when files are compacted, so compaction isn't a breaking operation for downstream streaming customers.

## Compacting Databricks Delta lakes

Databricks Delta and Delta Lake are different technologies. You need to pay for Databricks Delta whereas Delta Lake is free.

Databricks Delta lakes have an `OPTIMIZE` command that is not available for Delta Lakes and probably won't be in the future.

[From Michael Armbrust](https://github.com/delta-io/delta/issues/49):

> At this point, there are no plans to open-source the OPTIMIZE command, as the actual implementation is pretty deeply tied to other functionality that is only present in Databricks Runtime.

## Compacting partitioned Delta lakes

Here's how to compact the data in a single partition of a partitioned Delta lake ([answer is described here](https://github.com/delta-io/delta/issues/49)).

Suppose our data is stored in the `/some/path/data` folder and is partitioned by the `year` field. Further suppose that the `2019` directory contains 5 files and we'd like to compact it to one file.

Here's how we can compact the `2019` partition.

```
val table = "/some/path/data"
val partition = "year = '2019'"
val numFiles = 1

spark.read
  .format("delta")
  .load(table)
  .where(partition)
  .repartition(numFiles)
  .write
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere", partition)
  .save(table)
```

After compacting, the `00000000000000000001.json` file will contain one row like this.

```
{
  "add":{
    "path":"year=2019/part-00000-some-stuff.c000.snappy.parquet",
    "partitionValues":{
      "year":"2019"
    },
    "size":57516,
    "modificationTime":1567645788000,
    "dataChange":true
  }
}
```

The `00000000000000000001.json` file will also contain five rows like this.

```
{
  "remove":{
    "path":"year=2019/part-uncompacted-data.c000.snappy.parquet",
    "deletionTimestamp":1567645789342,
    "dataChange":true
  }
}
```

## Next steps

Delta makes compaction easy and it's going to get even better when users have the option to set the dataChange flag to false, so compaction isn't breaking for streaming customers.

Keep studying the transaction log to learn more about how Delta works!
