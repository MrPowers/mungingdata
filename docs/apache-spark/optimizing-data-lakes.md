---
title: "Optimizing Data Lakes for Apache Spark"
date: "2018-12-09"
categories: 
  - "apache-spark"
---

Spark code will run faster with certain data lakes than others.

For example, Spark will run slowly if the data lake uses gzip compression and has unequally sized files (especially if there are a lot of small files). The code will run fast if the data lake contains equally sized 1GB Parquet files that use snappy compression.

This blog post outlines the data lake characteristics that are desirable for Spark analyses.

1. File formats
2. Compression algorithms
3. Small file problem
4. Partitioning schemes

TL;DR:

- Use 1GB Parquet files with Snappy compression
- Solving the small file problem is important
- Partitioning data lakes is important

## File formats

Spark works with many file formats including Parquet, CSV, JSON, OCR, Avro, and text files.

> [TL;DR Use Apache Parquet instead of CSV or JSON whenever possible, because it's faster and better](http://garrens.com/blog/2017/10/09/spark-file-format-showdown-csv-vs-json-vs-parquet/).

[JSON is the worst file format for distributed systems](https://stackoverflow.com/a/38552139/1125159) and should be avoided whenever possible.

### Row vs. Column oriented formats

CSV, JSON, and Avro are row oriented data formats.

OCR and Parquet are column oriented data formats.

Row oriented file formats require data from all the rows to be transmitted over the wire for every analysis.

Suppose you have the following DataFrame (`df`) and would like to query the `city` column.

```
+-------+---------+----------+
|   city|  country|population|
+-------+---------+----------+
| Boston|      USA|      0.67|
|  Dubai|      UAE|       3.1|
|Cordoba|Argentina|      1.39|
+-------+---------+----------+
```

If the data is persisted as a Parquet file, then `df.select("city")` will only have to transmit one column worth of data across the wire.

If the data is persisted as a CSV file, then `df.select("city")` will transmit all the data across the wire.

### Lazy vs eager evaluation

As discussed in [this blog post](http://garrens.com/blog/2017/10/09/spark-file-format-showdown-csv-vs-json-vs-parquet/) CSV files are sometimes eagerly evaluated so Spark needs to perform a slow process to infer the schema (JSON files are always eagerly evaluated).

The Parquet file format makes it easy to avoid eager evaluation.

Spark can easily determine the schema of Parquet files from metadata, so it doesn't need to go through the time consuming process of reading files and inferring the schema.

### Ease of compression

Column oriented file formats are [more compressible](http://db.csail.mit.edu/projects/cstore/abadi-sigmod08.pdf):

> Intuitively, data stored in columns is more compressible than data stored in rows. Compression algorithms perform better on data with low information entropy (high data value locality). Take, for example, a database table containing information about customers (name, phone number, e-mail address, snail-mail address, etc.). Storing data in columns allows all of the names to be stored together, all of the phone numbers together, etc. Certainly phone numbers are more similar to each other than surrounding text fields like e-mail addresses or names. Further, if the data is sorted by one of the columns, that column will be super-compressible (for example, runs of the same value can be run-length encoded).

### Splittable compression algorithms

Files can be compressed with gzip, lzo, bzip2 and other compression algorithms.

gzipped files can't be split, so they're not ideal as described in [this blog post](http://garrens.com/blog/2017/11/04/big-data-spark-and-its-small-files-problem/).

bzip2 files are splittable, but they are expensive from a CPU perspective. [This blogger](http://aseigneurin.github.io/2016/11/08/spark-file-formats-and-storage-options.html) decided to go with uncompressed files after looking into the gzip and bzip2 options!

[Snappy](https://google.github.io/snappy/) is a different type of compression algorithm that "aims for very high speeds and reasonable compression". Snappy is also a splittable ([there are some nuances](https://stackoverflow.com/questions/32382352/is-snappy-splittable-or-not-splittable), but you can think of Snappy as splittable).

The Snappy compression algorithm is used by default in Spark and you should use Snappy unless you have a good reason to deviate from the Spark standard.

## Small file problem

S3 is an object store and listing files takes a long time. S3 does not list files quickly like a Unix-like object store.

Listing files is even slower when you glob, e.g. `spark.read.parquet("/mnt/my-bucket/lake/*/*")`.

For large data lakes, it's [ideal to use evenly sized 1 GB Parquet files](https://forums.databricks.com/questions/101/what-is-an-optimal-size-for-file-partitions-using.html). You'll want to create a process to periodically compact the small files into 1 GB Parquet files to keep the small file problem under control.

Incremental updates tend to create lots of small files. The more frequently the data lake is incrementally updated, the more rapidly small files will accumulate.

## Disk partitioning

You might want to partition your data on disk if you're frequently filtering on a certain column. Suppose you have a DataFrame (`df`) with `country`, `name`, and `date_of_birth` columns. You can create a data lake that's partitioned by `country` if you're frequently writing queries like `df.filter($"country" === "China")`.

Read [this blog post](https://www.mungingdata.com/apache-spark/partition-filters-pushed-filters) for more information on creating and querying partitioned data lakes.

A partitioned data lake can cause queries to run 100 times+ faster - don't overlook this important feature.

## Multiple data lakes

You might need to store the same data in different lakes that are optimized for different types of queries. Partitioned data lakes are great for queries that filter on a partitioned column, but they tend to have a lot of files and will be slower for other queries.

We'll want to use a unpartitioned lake for queries like this: `unpartitionedLakeDF.filter($"date_of_birth" === "2018-01-01")`.

As discussed in the previous section, a data lake that's partitioned on the `country` column is perfect for queries like this: `countryPartitionedDF.filter($"country" === "China")`.

## Conclusion

Properly designed data lakes will save your company a lot of time and money. I've seen queries that run 100 times faster and cheaper on a partitioned data lake for example.

This blog post explains common problems found in data lakes, but doesn't explain how to implement the solutions. These questions are still open.

- How should data lakes get updated incrementally?
- How should small files get compacted into larger files?
- How can multiple data lakes get updated simultaneously?

We'll dive into these details in future posts.
