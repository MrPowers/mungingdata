---
title: "Writing out single files with Spark (CSV or Parquet)"
date: "2020-06-18"
categories: 
  - "apache-spark"
---

# Writing out single files with Spark (CSV or Parquet)

This blog explains how to write out a DataFrame to a single file with Spark. It also describes how to write out data in a file with a specific name, which is surprisingly challenging.

Writing out a single file with Spark isn't typical. Spark is designed to write out multiple files in parallel. Writing out many files at the same time is faster for big datasets.

## Default behavior

Let's create a DataFrame, use `repartition(3)` to create three memory partitions, and then write out the file to disk.

```scala
val df = Seq("one", "two", "three").toDF("num")
df
  .repartition(3)
  .write.csv(sys.env("HOME")+ "/Documents/tmp/some-files")
```

Here's the files that are generated on disk.

```
Documents/
  tmp/
    some-files/
      _SUCCESS
      part-00000-b69460e8-fdc3-4593-bab4-bd15fa0dad98-c000.csv
      part-00001-b69460e8-fdc3-4593-bab4-bd15fa0dad98-c000.csv
      part-00002-b69460e8-fdc3-4593-bab4-bd15fa0dad98-c000.csv
```

Spark writes out one file per memory partition. We used `repartition(3)` to create three memory partitions, so three files were written.

## Writing out one file with repartition

We can use `repartition(1)` write out a single file.

```scala
df
  .repartition(1)
  .write.csv(sys.env("HOME")+ "/Documents/tmp/one-file-repartition")
```

Here's the file that's written to disk.

```
Documents/
  tmp/
    one-file-repartition/
      _SUCCESS
      part-00000-d5a15f40-e787-4fd2-b8eb-c810d973b6fe-c000.csv
```

We can't control the name of the file that's written. We can control the name of the directory, but not the file itself.

This solution isn't sufficient when you want to write data to a file with a specific name.

## Writing out a single file with coalesce

We can also use `coalesce(1)` to write out a single file.

```scala
df
  .coalesce(1)
  .write.csv(sys.env("HOME")+ "/Documents/tmp/one-file-coalesce")
```

Here's what's outputted.

```
Documents/
  tmp/
    one-file-coalesce/
      _SUCCESS
      part-00000-c7521799-e6d8-498d-b857-2aba7f56533a-c000.csv
```

coalesce doesn't let us set a specific filename either (it only let's us customize the folder name). We'll need to use [spark-daria](https://github.com/MrPowers/spark-daria/) to access a method that'll output a single file.

## Writing out a file with a specific name

You can use the `DariaWriters.writeSingleFile` function defined in [spark-daria](https://github.com/MrPowers/spark-daria/) to write out a single file with a specific filename.

Here's the code that writes out the contents of a DataFrame to the `~/Documents/better/mydata.csv` file.

```scala
import com.github.mrpowers.spark.daria.sql.DariaWriters

DariaWriters.writeSingleFile(
    df = df,
    format = "csv",
    sc = spark.sparkContext,
    tmpFolder = sys.env("HOME") + "/Documents/better/tmp",
    filename = sys.env("HOME") + "/Documents/better/mydata.csv"
)
```

The `writeSingleFile` method let's you name the file without worrying about complicated implementation details.

`writeSingleFile` is uses `repartition(1)` and Hadoop filesystem methods underneath the hood. All of the Hadoop filesystem methods are available in any Spark runtime environment - you don't need to attach any separate JARs.

## Compatibility with other filesystems

It's best to use the Hadoop filesystem methods when moving, renaming, or deleting files, so your code will work on multiple platforms. `writeSingleFile` works on your local filesystem and in S3. You can use this approach when running Spark locally or in a Databricks notebook.

There are other solutions to this problem that are not cross platform. There are solutions that only work in Databricks notebooks, or only work in S3, or only work on a Unix-like operating system.

The Hadoop filesystem methods are clumsy to work with, but the best option cause they work on multiple platforms.

The `writeSingleFile` method uses the `fs.rename()` Hadoop method, [as described in this answer](https://stackoverflow.com/a/48223470/1125159). Here's the psuedocode:

```java
val src = new Path("s3a://bucket/data/src")
val dest = new Path("s3a://bucket/data/dest")
val conf = sc.hadoopConfiguration   // assuming sc = spark context
val fs = src.getFileSystem(conf)
fs.rename(src, dest)
```

## copyMerge

Hadoop 2 has a `FileUtil.copyMerge()` method that's an elegant solution to this problem, but [this method is deprecated and will be removed in Hadoop 3](https://stackoverflow.com/a/34792366/1125159). There is [an answer in this thread](https://stackoverflow.com/questions/42035735/how-to-do-copymerge-in-hadoop-3-0) that reimplements `copyMerge` for Hadoop 3 users.

In any case, don't write code that relies on the `FileUtil.copyMerge()` method. We know that method will be inaccessible when Spark upgrades to Hadoop 3 and you don't want to rely on a deprecated method that'll break at some unknown time in the future.

## Next steps

You'll typically want to write out multiple files in parallel, but in the rare occasions when you want to write out a single file, the [spark-daria](https://github.com/MrPowers/spark-daria/) `writeSingleFile` method will help.

Try your best to wrap the complex Hadoop filesystem logic in helper methods that are tested separated. Combining Hadoop filesystem operations and Spark code in the same method will make your code too complex.
