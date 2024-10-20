---
title: "Partitioning on Disk with partitionBy"
date: "2019-10-19"
categories: 
  - "apache-spark"
---

Spark writers allow for data to be partitioned on disk with `partitionBy`. Some queries can run 50 to 100 times faster on a partitioned data lake, so partitioning is vital for certain queries.

Creating and maintaining partitioned data lake is hard.

This blog post discusses how to use `partitionBy` and explains the challenges of partitioning production-sized datasets on disk. Different memory partitioning tactics will be discussed that let `partitionBy` operate more efficiently.

You'll need to master the concepts covered in this blog to create partitioned data lakes on large datasets, especially if you're dealing with a high cardinality or high skew partition key.

Make sure to read [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) for a detailed overview of how to create production grade partitioned lakes.

## Memory partitioning vs. disk partitioning

`coalesce()` and `repartition()` change the memory partitions for a DataFrame.

`partitionBy()` is a `DataFrameWriter` method that specifies if the data should be written to disk in folders. By default, Spark does not write data to disk in nested folders.

Memory partitioning is often important independent of disk partitioning. In order to write data on disk properly, you'll almost always need to repartition the data in memory first.

## Simple example

Suppose we have the following CSV file with `first_name`, `last_name`, and `country` columns:

```
first_name,last_name,country
Ernesto,Guevara,Argentina
Vladimir,Putin,Russia
Maria,Sharapova,Russia
Bruce,Lee,China
Jack,Ma,China
```

Let's partition this data on disk with `country` as the partition key. Let's create one file per partition.

```
val path = new java.io.File("./src/main/resources/ss_europe/").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/partitioned_lake1/").getCanonicalPath
df
  .repartition(col("country"))
  .write
  .partitionBy("country")
  .parquet(outputPath)
```

Here's what the data will look like on disk:

```
partitioned_lake1/
  country=Argentina/
    part-00044-cf737804-90ea-4c37-94f8-9aa016f6953a.c000.snappy.parquet
  country=China/
    part-00059-cf737804-90ea-4c37-94f8-9aa016f6953a.c000.snappy.parquet
  country=Russia/
    part-00002-cf737804-90ea-4c37-94f8-9aa016f6953a.c000.snappy.parquet
```

Creating one file per disk partition is not going to work for production sized datasets. Suppose the China partition contains 100GB of data - we won't be able to write out all of that data in a single file.

## partitionBy with repartition(5)

Let's run `repartition(5)` to get each row of data in a separate memory partition before running `partitionBy` and see how that impacts how the files get written to disk.

```
val outputPath = new java.io.File("./tmp/partitioned_lake2/").getCanonicalPath
df
  .repartition(5)
  .write
  .partitionBy("country")
  .parquet(outputPath)
```

Here's what the files look like on disk:

```
partitioned_lake2/
  country=Argentina/
    part-00003-c2d1b76a-aa61-437f-affc-a6b322f1cf42.c000.snappy.parquet
  country=China/
    part-00000-c2d1b76a-aa61-437f-affc-a6b322f1cf42.c000.snappy.parquet
    part-00004-c2d1b76a-aa61-437f-affc-a6b322f1cf42.c000.snappy.parquet
  country=Russia/
    part-00001-c2d1b76a-aa61-437f-affc-a6b322f1cf42.c000.snappy.parquet
    part-00002-c2d1b76a-aa61-437f-affc-a6b322f1cf42.c000.snappy.parquet
```

The `partitionBy` writer will write out files on disk for each memory partition. The maximum number of files written out is the number of unique countries multiplied by the number of memory partitions.

In this example, we have 3 unique countries \* 5 memory partitions, so up to 15 files could get written out (if each memory partition had one Argentinian, one Chinese, and one Russian person). We only have 5 rows of data, so only 5 files are written in this example.

## partitionBy with repartition(1)

If we repartition the data to one memory partition before partitioning on disk with `partitionBy`, then we'll write out a maximum of three files. numMemoryPartitions \* numUniqueCountries = maxNumFiles. 1 \* 3 = 3.

Let's take a look at the code.

```
val outputPath = new java.io.File("./tmp/partitioned_lake2/").getCanonicalPath
df
  .repartition(1)
  .write
  .partitionBy("country")
  .parquet(outputPath)
```

Here's what the files look like on disk:

```
partitioned_lake3/
  country=Argentina/
    part-00000-bc6ce757-d39f-489e-9677-0a7105b29e66.c000.snappy.parquet
  country=China/
    part-00000-bc6ce757-d39f-489e-9677-0a7105b29e66.c000.snappy.parquet
  country=Russia/
    part-00000-bc6ce757-d39f-489e-9677-0a7105b29e66.c000.snappy.parquet
```

## Partitioning datasets with a max number of files per partition

Let's use a dataset with 80 people from China, 15 people from France, and 5 people from Cuba. Here's [a link to the data](https://gist.github.com/MrPowers/95a8e160c37fffa9ffec2f9acfbee51e).

Here's what the data looks like:

```
person_name,person_country
a,China
b,China
c,China
...77 more China rows
a,France
b,France
c,France
...12 more France rows
a,Cuba
b,Cuba
c,Cuba
...2 more Cuba rows
```

Let's create 8 memory partitions and scatter the data randomly across the memory partitions (we'll write out the data to disk, so we can inspect the contents of a memory partition).

```
val outputPath = new java.io.File("./tmp/repartition_for_lake4/").getCanonicalPath
df
  .repartition(8, col("person_country"), rand)
  .write
  .csv(outputPath)
```

Let's look at one of the CSV files that is outputted:

```
p,China
f1,China
n1,China
a2,China
b2,China
d2,China
e2,China
f,France
c,Cuba
```

This technique helps us set a maximum number of files per partition when creating a partitioned lake. Let's write out the data to disk and observe the output.

```
val outputPath = new java.io.File("./tmp/partitioned_lake4/").getCanonicalPath
df
  .repartition(8, col("person_country"), rand)
  .write
  .partitionBy("person_country")
  .csv(outputPath)
```

Here's what the files look like on disk:

```
partitioned_lake4/
  person_country=China/
    part-00000-0887fbd2-4d9f-454a-bd2a-de42cf7e7d9e.c000.csv
    part-00001-0887fbd2-4d9f-454a-bd2a-de42cf7e7d9e.c000.csv
    ... 6 more files
  person_country=Cuba/
    part-00002-0887fbd2-4d9f-454a-bd2a-de42cf7e7d9e.c000.csv
    part-00003-0887fbd2-4d9f-454a-bd2a-de42cf7e7d9e.c000.csv
    ... 2 more files
  person_country=France/
    part-00000-0887fbd2-4d9f-454a-bd2a-de42cf7e7d9e.c000.csv
    part-00001-0887fbd2-4d9f-454a-bd2a-de42cf7e7d9e.c000.csv
    ... 5 more files
```

Each disk partition will have up to 8 files. The data is split randomly in the 8 memory partitions. There won't be any output files for a given disk partition if the memory partition doesn't have any data for the country.

This is better, but still not ideal. We have 4 files for Cuba and seven files for France, so too many small files are being created.

Let's review the contents of our memory partition from earlier:

```
p,China
f1,China
n1,China
a2,China
b2,China
d2,China
e2,China
f,France
c,Cuba
```

`partitionBy` will split up this particular memory partition into three files: one China file with 7 rows of data, one France file with one row of data, and one Cuba file with one row of data.

## Partitioning dataset with max rows per file

Let's write some code that'll create partitions with ten rows of data per file. We'd like our data to be stored in 8 files for China, one file for Cuba, and two files for France.

We can use the `maxRecordsPerFile` option to output files with 10 rows.

```
val outputPath = new java.io.File("./tmp/partitioned_lake5/").getCanonicalPath
df
  .repartition(col("person_country"))
  .write
  .option("maxRecordsPerFile", 10)
  .partitionBy("person_country")
  .csv(outputPath)
```

This technique is particularity important for partition keys that are highly skewed. The number of inhabitants by country is a good example of a partition key with high skew. For example Jamaica has 3 million people and China has 1.4 billion people - we'll want ~467 times more files in the China partition than the Jamaica partition.

## Partitioning dataset with max rows per file pre Spark 2.2

The maxRecordsPerFile option was added in Spark 2.2, so you'll need to write your own custom solution if you're using an earlier version of Spark.

```
val countDF = df.groupBy("person_country").count()

val desiredRowsPerPartition = 10

val joinedDF = df
  .join(countDF, Seq("person_country"))
  .withColumn(
    "my_secret_partition_key",
    (rand(10) * col("count") / desiredRowsPerPartition).cast(IntegerType)
  )

val outputPath = new java.io.File("./tmp/partitioned_lake6/").getCanonicalPath
joinedDF
  .repartition(col("person_country"), col("my_secret_partition_key"))
  .drop("count", "my_secret_partition_key")
  .write
  .partitionBy("person_country")
  .csv(outputPath)
```

We calculate the total number of records per partition key and then create a `my_secret_partition_key` column rather than relying on a fixed number of partitions.

You should choose the `desiredRowsPerPartition` based on what will give you ~1 GB files. If you have a 500 GB dataset with 750 million rows, set `desiredRowsPerPartition` to 1,500,000.

## Small file problem

Partitioned data lakes can quickly develop a small file problem when they're updated incrementally. It's hard to compact partitioned data lakes. As we've seen, it's even hard to make a partitioned data lake!

Use the tactics outlined in this blog post to build your partitioned data lakes and start them off without the small file problem!

## Conclusion

Partitioned data lakes can be much faster to query (when filtering on the partition keys) because they allow for a massive data skipping.

Creating and maintaining partitioned data lakes is challenging, but the performance gains make them a worthwhile effort.
