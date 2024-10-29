---
title: "Compacting Parquet Files"
date: "2019-09-03"
categories: 
  - "apache-spark"
---

# Compacting Parquet Files

This post describes how to programatically compact Parquet files in a folder.

Incremental updates frequently result in lots of small files that can be slow to read. It's best to periodically compact the small files into larger files, so they can be read faster.

## TL;DR

You can easily compact Parquet files in a folder with the [spark-daria](https://github.com/MrPowers/spark-daria) `ParquetCompactor` class. Suppose you have a folder with a thousand 11 MB files that you'd like to compact into 20 files. Here's the code that'll perform the compaction.

```scala
import com.github.mrpowers.spark.daria.sql.ParquetCompactor

new ParquetCompactor("/path/to/the/data", 20).run()
```

## Compaction steps

Here are the high level compaction steps:

1. List all of the files in the directory
2. Coalesce the files
3. Write out the compacted files
4. Delete the uncompacted files

Let's walk through the [spark-daria](https://github.com/MrPowers/spark-daria) compaction code to see how the files are compacted.

Start by writing all the uncompacted filenames in the folder to a separate directory. We'll use this filename listing to delete all the uncompacted files later.

```scala
val df = spark.read.parquet(dirname)

df.withColumn("input_file_name_part", regexp_extract(input_file_name(), """part.+c\d{3}""", 0))
  .select("input_file_name_part")
  .distinct
  .write
  .parquet(s"$dirname/input_file_name_parts")
```

Let's read in all the uncompacted data into a DataFrame, coalesce the data into `numOutputFiles` partitions, and then write out the partitioned data.

```scala
val fileNames = spark.read
  .parquet(s"$dirname/input_file_name_parts")
  .collect
  .map((r: Row) => r(0).asInstanceOf[String])

val uncompactedDF = spark.read
  .parquet(s"$dirname/{${fileNames.mkString(",")}}*.parquet")

uncompactedDF
  .coalesce(numOutputFiles)
  .write
  .mode(SaveMode.Append)
  .parquet(dirname)
```

Our data lake now contains the unpartitioned files and the compacted files. We have the same data stored twice.

Let's delete all of the unpartitioned files and then delete the directory that was tracking the unpartitioned file names.

```scala
import org.apache.hadoop.fs.{FileSystem, Path}

val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

fileNames.foreach { filename: String =>
  fs.delete(new Path(s"$dirname/$filename.snappy.parquet"), false)
}

fs.delete(new Path(s"$dirname/input_file_name_parts"), true)
```

The optimal number of partitions depends on how much data is stored in the folder. It's generally best to use 1GB files. If you're folder contains 260GB of data, you should use 260 partitions.

## Programatically computing folder sizes

We can infer the optimal number of files for a folder by how much data is in the folder.

The AWS CLI makes it easy to calculate the number of data in a folder with this command.

```
aws s3 ls --summarize --human-readable --recursive s3://bucket-name/directory
```

Accessing the AWS CLI via your Spark runtime isn't always the easiest, so you can also use some `org.apache.hadoop` code. The code returns the number of bytes in a folder.

```scala
val filePath   = new org.apache.hadoop.fs.Path(dirname)
val fileSystem = filePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
fileSystem.getContentSummary(filePath).getLength
```

## Compacting partitioned lakes

Let's iterate over every partition in a partitioned data lake and compact each partition:

```scala
import new com.github.mrpowers.spark.daria.sql.ParquetCompactor
import com.github.mrpowers.spark.daria.utils.DirHelpers

val dirname = "/some/path"
val partitionNames = Array("partition1", "partition2)
partitionNames.foreach{ p: String =>
  val numBytes = DirHelpers.numBytes(s"$dirname/$p")
  val numGigaBytes = DirHelpers.bytesToGb(numBytes)
  val num1GBPartitions = DirHelpers.num1GBPartitions(numGigaBytes)

  new ParquetCompactor(s"$dirname/$p", num1GBPartitions).run()
}
```

We calculate the number of gigabytes in each partition and use that to set the optimal number of files per partition. This script only compacts one partition at a time, so it shouldn't overload a cluster.

## Conclusion

Compacting Parquet data lakes is important so the data lake can be read quickly.

Compaction is particularly important for partitioned Parquet data lakes that tend to have tons of files.

Use the tactics in this blog to keep your Parquet files close to the 1GB ideal size and keep your data lake read times fast.
