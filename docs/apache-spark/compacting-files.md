---
title: "Compacting Files with Spark to Address the Small File Problem"
date: "2018-10-21"
categories: 
  - "apache-spark"
---

Spark runs slowly when it reads data from a lot of small files in S3. You can make your Spark code run faster by creating a job that compacts small files into larger files.

The “small file problem” is especially problematic for data stores that are updated incrementally. The small problem get progressively worse if the incremental updates are more frequent and the longer incremental updates run between full refreshes.

[Garren Staubli wrote a great blog](http://garrens.com/blog/2017/11/04/big-data-spark-and-its-small-files-problem/) does a great job explaining why small files are a big problem for Spark analyses. This blog will describe how to get rid of small files using Spark.

## Simple example

Let's look at a folder with some small files (we'd like all the files in our data lake to be 1GB):

- File A: 0.5 GB
- File B: 0.5 GB
- File C: 0.1 GB
- File D: 0.2 GB
- File E: 0.3 GB
- File F: 1 GB
- File G: 1 GB
- File H: 1 GB

Our folder has 4.6 GB of data.

Let's use the `repartition()` method to shuffle the data and write it to another directory with five 0.92 GB files.

```
val df = spark.read.parquet("s3_path_with_the_data")
val repartitionedDF = df.repartition(5)
repartitionedDF.write.parquet("another_s3_path")
```

The `repartition()` method makes it easy to build a folder with equally sized files.

## Only repartitioning the small files

Files F, G, and H are already perfectly sized, so it'll be more performant to simply repartition Files A, B, C, D, and E (the small files).

The small files contain 1.6 GB of data. We can read in the small files, write out 2 files with 0.8 GB of data each, and then delete all the small files. Let's take a look at some pseudocode.

```
val df = spark.read.parquet("fileA, fileB, fileC, fileD, fileE")
val newDF = df.repartition(2)
newDF.write.parquet("s3_path_with_the_data")
// run a S3 command to delete fileA, fileB, fileC, fileD, fileE
```

Here's what `s3_path_with_the_data` will look like after the small files have been compacted.

- File F: 1 GB
- File G: 1 GB
- File H: 1 GB
- File I: 0.8 GB
- File J: 0.8 GB

This approach is nice because the data isn't written to a new directory. All of our code that references with `s3_path_with_the_data` will still work.

## Real code to repartition the small files

Kaggle has an [open source CSV hockey dataset](https://www.kaggle.com/martinellis/nhl-game-data/version/1) called `game_shifts.csv` that has 5.56 million rows of data and 5 columns.

Let's split up this CSV into 6 separate files and store them in the `nhl_game_shifts` S3 directory:

- game\_shiftsA.csv: 68.7 MB
- game\_shiftsB.csv: 68.7 MB
- game\_shiftsC.csv: 51.5 MB
- game\_shiftsD.csv: 1.0 MB
- game\_shiftsE.csv: 0.5 MB
- game\_shiftsF.csv: 0.7 MB

Let's read game\_shiftsC, game\_shiftsD, game\_shiftsE, and game\_shiftsF into a DataFrame, shuffle the data to a single partition, and write out the data as a single file.

```
import org.apache.spark.sql.SaveMode

val smallFilesDF = spark.read
.option("header", "true")
.csv(s"/mnt/some-bucket/nhl_game_shifts/{game_shiftsC.csv,game_shiftsD.csv,game_shiftsE.csv,game_shiftsF.csv}")

smallFilesDF
.repartition(1)
.write
.mode(SaveMode.Append)
.csv("/mnt/some-bucket/nhl_game_shifts")
```

Let's run some AWS CLI commands to delete files C, D, E, and F.

```
aws s3 rm s3://some-bucket/nhl_game_shifts/game_shiftsC.csv
aws s3 rm s3://some-bucket/nhl_game_shifts/game_shiftsD.csv
aws s3 rm s3://some-bucket/nhl_game_shifts/game_shiftsE.csv
aws s3 rm s3://some-bucket/nhl_game_shifts/game_shiftsF.csv
```

Here's what `s3://some-bucket/nhl_game_shifts` contains after this code is run:

- game\_shiftsA.csv: 68.7 MB
- game\_shiftsB.csv: 68.7 MB
- part-00000-tid-…-c000.csv: 53.7 MB

## Programatically compacting the small files

Let's use the AWS CLI to identify the small files in a S3 folder.

Need to finish the rest of this section…

## Small file problem in Hadoop

Hadoop's small file problem has been [well documented for quite some time](https://snowplowanalytics.com/blog/2013/05/30/dealing-with-hadoops-small-files-problem/). Cloudera does a great job [examining this problem as well](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/).

## Next steps

It's important to quantify how many small data files are contained in folders that are queried frequently.

If there are folders with a lot of small files, you should compact the files and see if that improves query performance.

Eliminating small files can significanly improve performance.
