---
title: "Writing Parquet Files in Python with Pandas, PySpark, and Koalas"
date: "2020-03-29"
categories: 
  - "python"
---

This blog post shows how to convert a CSV file to Parquet with Pandas, Spark, [PyArrow](https://mungingdata.com/pyarrow/parquet-metadata-min-max-statistics/) and [Dask](https://mungingdata.com/dask/read-csv-to-parquet/).

It discusses the pros and cons of each approach and explains how both approaches can happily coexist in the same ecosystem.

Parquet is a columnar file format whereas CSV is row based. Columnar file formats are more efficient for most analytical queries. You can speed up a lot of your Panda DataFrame queries by converting your CSV files and working off of Parquet files.

All the code used in this blog is in [this GitHub repo](https://github.com/MrPowers/python-parquet-examples).

## Pandas approach

Suppose you have the following `data/us_presidents.csv` file:

```
full_name,birth_year
teddy roosevelt,1901
abe lincoln,1809
```

You can easily read this file into a Pandas DataFrame and write it out as a Parquet file [as described in this Stackoverflow answer](https://stackoverflow.com/questions/50604133/convert-csv-to-parquet-file-using-python).

```
import pandas as pd

def write_parquet_file():
    df = pd.read_csv('data/us_presidents.csv')
    df.to_parquet('tmp/us_presidents.parquet')

write_parquet_file()
```

This code writes out the data to a `tmp/us_presidents.parquet` file.

Let's read the Parquet data into a Pandas DataFrame and view the results.

```
df = pd.read_parquet('tmp/us_presidents.parquet')
print(df)

         full_name  birth_year
0  teddy roosevelt        1901
1      abe lincoln        1809
```

Pandas provides a beautiful Parquet interface. Pandas leverages the PyArrow library to write Parquet files, but you can also write Parquet files directly from PyArrow.

## PyArrow

PyArrow lets you read a CSV file into a table and write out a Parquet file, [as described in this blog post](https://mungingdata.com/pyarrow/parquet-metadata-min-max-statistics/). The code is simple to understand:

```
import pyarrow.csv as pv
import pyarrow.parquet as pq

table = pv.read_csv('./data/people/people1.csv')
pq.write_table(table, './tmp/pyarrow_out/people1.parquet')
```

PyArrow is worth learning because it provides access to file schema and other metadata stored in the Parquet footer. Studying PyArrow will teach you more about Parquet.

## Dask

Dask is a parallel computing framework that makes it easy to convert a lot of CSV files to Parquet files with a single operation [as described in this post](https://mungingdata.com/dask/read-csv-to-parquet/).

Here's a code snippet, but you'll need to read the blog post to fully understand it:

```
import dask.dataframe as dd

df = dd.read_csv('./data/people/*.csv')
df.to_parquet('./tmp/people_parquet2', write_index=False)
```

Dask is similar to Spark and easier to use for folks with a Python background. Spark is still worth investigating, especially because it's so powerful for big data sets.

## PySpark

Let's read the CSV data to a PySpark DataFrame and write it out in the Parquet format.

We'll start by creating a `SparkSession` that'll provide us access to the Spark CSV reader.

```
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("parquet_example") \
  .getOrCreate()

df = spark.read.csv('data/us_presidents.csv', header = True)
df.repartition(1).write.mode('overwrite').parquet('tmp/pyspark_us_presidents')
```

We need to specify `header = True` when reading the CSV to indicate that the first row of data is column headers.

Spark normally writes data to a directory with many files. The directory only contains one file in this example because we used `repartition(1)`. Spark can write out multiple files in parallel for big datasets and that's one of the reasons Spark is such a powerful big data engine.

Let's look at the contents of the `tmp/pyspark_us_presidents` directory:

```
pyspark_us_presidents/
  _SUCCESS
  part-00000-81610cf2-dc76-481e-b302-47b59e06d9b6-c000.snappy.parquet
```

The `part-00000-81...snappy.parquet` file contains the data. Spark uses the Snappy compression algorithm for Parquet files by default.

Let's read `tmp/pyspark_us_presidents` Parquet data into a DataFrame and print it out.

```
df = spark.read.parquet('tmp/pyspark_us_presidents')
df.show()

+---------------+----------+
|      full_name|birth_year|
+---------------+----------+
|teddy roosevelt|      1901|
|    abe lincoln|      1809|
+---------------+----------+
```

Setting up a PySpark project on your local machine is surprisingly easy, see [this blog post](https://mungingdata.com/pyspark/poetry-dependency-management-wheel/) for details.

## Koalas

[koalas](https://github.com/databricks/koalas/) lets you use the Pandas API with the Apache Spark execution engine under the hood.

Let's read the CSV and write it out to a Parquet folder (notice how the code looks like Pandas):

```
import databricks.koalas as ks

df = ks.read_csv('data/us_presidents.csv')
df.to_parquet('tmp/koala_us_presidents')
```

Read the Parquet output and display the contents:

```
df = ks.read_parquet('tmp/koala_us_presidents')
print(df)

         full_name  birth_year
0  teddy roosevelt        1901
1      abe lincoln        1809
```

Koalas outputs data to a directory, similar to Spark. Here's what the `tmp/koala_us_presidents` directory contains:

```
koala_us_presidents/
  _SUCCESS
  part-00000-1943a0a6-951f-4274-a914-141014e8e3df-c000.snappy.parquet
```

## Pandas and Spark can happily coexist

Pandas is great for reading relatively small datasets and writing out a single Parquet file.

Spark is great for reading and writing huge datasets and processing tons of files in parallel.

Suppose your data lake currently contains 10 terabytes of data and you'd like to update it every 15 minutes. You get 100 MB of data every 15 minutes. Maybe you setup a lightweight Pandas job to incrementally update the lake every 15 minutes. You can do the big extracts and data analytics on the whole lake with Spark.

## Next steps

The [Delta Lake project](https://delta.io/) makes Parquet data lakes a lot more powerful by adding a transaction log. This makes it easier to perform operations like backwards compatible compaction, etc.

I am going to try to make an open source project that makes it easy to interact with Delta Lakes from Pandas. The Delta lake design philosophy should make it a lot easier for Pandas users to manage Parquet datasets. Stay tuned!
