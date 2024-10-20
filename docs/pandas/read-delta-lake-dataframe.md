---
title: "Reading Delta Lakes into pandas DataFrames"
date: "2021-10-11"
categories: 
  - "pandas"
---

This post explains how to read Delta Lakes into pandas DataFrames.

The [delta-rs](https://github.com/delta-io/delta-rs) library makes this incredibly easy and doesn't require any Spark dependencies.

Let's look at some simple examples, explore when this is viable, and clarify the limitations of this approach.

Delta Lakes are better for managing data compared to vanilla CSV or Parquet lakes. Delta solves a lot of common data management problems, which saves users from addressing these concerns themselves. This post should excite pandas users with an easier way to manage datasets.

## Create Delta Lake

Let's start by creating a Delta Lake so we can read it into pandas DataFrames in our examples.

Use PySpark to create the a Delta Lake:

```
data = [("jose", 10), ("li", 12), ("luisa", 14)]
df = spark.createDataFrame(data, ["name", "num"])
df.write.format("delta").save("resources/delta/1")
```

Feel free to clone the [dask-interop](https://github.com/MrPowers/dask-interop) project and use the example Delta Lakes that are checked into the repo if you don't want to setup Spark on your local machine.

## Read Delta Lake into pandas

Here's how to read the Delta Lake into a pandas DataFrame.

```
from deltalake import DeltaTable

dt = DeltaTable("resources/delta/1")
df = dt.to_pandas()
print(df)

    name  num
0   jose   10
1     li   12
2  luisa   14
```

You need to run `pip install deltalake` to install the delta-rs library.

delta-rs makes it really easy to read a Delta Lake into a pandas table.

## Time travel

Delta Lakes also allow for time travel between different versions of the data. Time travel is great when you'd like to use a fixed dataset version for model training.

Let's build another Delta Lake with two transactions and demonstrate how to time travel between the two different versions of the data. Start by creating a Delta Lake with two write transactions.

```
data = [("a", 1), ("b", 2), ("c", 3)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.format("delta").save("resources/delta/2")

data = [("d", 4), ("e", 5), ("f", 6)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.mode("append").format("delta").save("resources/delta/2")
```

The first transaction wrote this data (version 0 of the dataset):

| letter | number |
| --- | --- |
| a | 1 |
| b | 2 |
| c | 3 |

The second transaction wrote this data (version 1 of the dataset):

| letter | number |
| --- | --- |
| d | 4 |
| e | 5 |
| f | 6 |

Read in the entire dataset to a pandas DataFrame.

```
dt = DeltaTable("resources/delta/2")
df = dt.to_pandas()
print(df)

  letter  number
0      a       1
1      b       2
2      c       3
3      d       4
4      e       5
5      f       6
```

Delta Lakes will read the latest version of the data by default.

Now time travel back to the first version of the data.

```
dt.load_version(0)
df = dt.to_pandas()
print(df)
```

```
  letter  number
0      a       1
1      b       2
2      c       3
```

You can keep adding additional data to the lake while maintaing easy access to a specific version of the data, which is useful when model training and debugging data weirdness. Vanilla Parquet lakes don't support time travel, which makes debugging painful.

delta-rs makes it really easy to time travel between different versions of the data.

## Schema Evolution

Delta also supports schema evolution which makes it possible to read Parquet files with different schemas into the same pandas DataFrame.

Perform two transactions to a Delta Lake, one that writes a two column dataset, and another that writes a 3 column dataset. Verify that Delta can use schema evolution to read the different Parquet files into a single pandas DataFrame.

Here's the data that'll be written with the two transactions.

First transaction:

| letter | number |
| --- | --- |
| a | 1 |
| b | 2 |
| c | 3 |

Second transaction:

| letter | number | color |
| --- | --- | --- |
| d | 4 | red |
| e | 5 | green |
| f | 6 | blue |

Here's the PySpark code to create the Delta Lake:

```
data = [("a", 1), ("b", 2), ("c", 3)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.format("delta").save("resources/delta/3")

data = [("d", 4, "red"), ("e", 5, "blue"), ("f", 6, "green")]
df = spark.createDataFrame(data, ["letter", "number", "color"])
df.write.mode("append").format("delta").save("resources/delta/3")
```

Let's read the Delta Lake into a pandas DataFrame and print the results.

```
dt = DeltaTable("resources/delta/3")
df = dt.to_pandas()
print(df)
```

```
  letter  number  color
0      a       1   None
1      b       2   None
2      c       3   None
3      d       4    red
4      e       5   blue
5      f       6  green
```

A normal Parquet reader cannot handle files that have different schemas. Delta allow for columns to be added to data lakes without having to rewrite existing files. This is a handy feature that delta-rs provides out of the box.

## Limitations of pandas

Delta Lakes are normally used for huge datasets and won't be readable into pandas DataFrames. The design pattern outlined in this post will only work for Delta Lakes that are relatively small.

If you want to read a large Delta lake, you'll need to use a cluster computing technology like Spark or Dask.

## Reading Delta Lakes with PySpark

You can also read Delta Lakes and convert them to pandas DataFrames with PySpark.

Here's an example:

```
pyspark_df = (
    spark.read.format("delta").option("mergeSchema", "true").load("resources/delta/3")
)
pandas_df = pyspark_df.toPandas()
```

This is the best approach if you have access to a Spark runtime. delta-rs is the best option if you don't have access to Spark (and want to avoid the heavy dependency).

You can't convert huge Delta Lakes to pandas DataFrames with PySpark either. When you convert a PySpark DataFrame to pandas, it collects all the data on the driver node and is bound by the memory of the driver node.

## Conclusion

Delta Lakes are almost always preferable to plain vanilla CSV or Parquet lakes. They allow for time travel, schema evolution, versioned data, and more.

This blog post demonstrates how easy it is to read Delta Lakes into pandas DataFrames with delta-rs.

Let's hope this post motivates the Python community to start using Delta Lakes so they can save time on "common data challenges" that can be offloaded to the file format! It'll be even easier to encourage the Python community to switch to Delta once delta-rs support writes to Delta via Pandas!
