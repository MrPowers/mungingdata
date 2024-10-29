---
title: "Reading Delta Lakes into Dask DataFrames"
date: "2021-12-13"
categories: 
  - "dask"
---

# Reading Delta Lakes into Dask DataFrames

This post explains how to read Delta Lakes into Dask DataFrames.  It shows how you can leverage powerful data lake management features like time travel, versioned data, and schema evolution with Dask.

Delta Lakes are normally written by Spark, but there are new projects like [delta-rs](https://github.com/delta-io/delta-rs) that provide Rust, Ruby, and Python bindings for Delta lakes.  delta-rs does not depend on Spark, so it doesn’t require Java or other heavy dependencies.

Let’s start by writing out a Delta Lake with PySpark and then reading it into a Dask DataFrame with delta-rs.

## Write Delta Lake & Read into Dask

Use PySpark to write a Delta Lake that has three rows of data.

```python
from pyspark.sql import SparkSession

from delta import *

builder = (
    SparkSession.builder.appName("dask-interop")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

data = [("jose", 10), ("li", 12), ("luisa", 14)]
df = spark.createDataFrame(data, ["name", "num"])
df.write.mode("overwrite").format("delta").save("resources/delta/1")

```

The resources/delta/1 directory contains Parquet files and a transaction log with metadata.

Read in the Delta table to a Dask DataFrame and print it to make sure it’s working properly.

```python
import dask.dataframe as dd
from deltalake import DeltaTable

dt = DeltaTable("resources/delta/1")
filenames = ["resources/delta/1/" + f for f in dt.files()]

ddf = dd.read_parquet(filenames, engine="pyarrow")

print(ddf.compute())

    name  num
0   jose   10
0     li   12
0  luisa   14

```

delta-rs makes it really easy to read Delta Lakes into Dask DataFrames.

## Understanding Delta Transaction Log

The best way to learn how Delta Lake works is by inspecting the filesystem and transaction log entries after performing transactions.  
Here are the files that are outputted when df.write.mode("overwrite").format("delta").save("resources/delta/1") is run.

```
resources/delta/1/
  _delta_log/
    00000000000000000000.json
  part-00000-193bf99f-66bf-4bbb-ab4c-868851bd5a24-c000.snappy.parquet
  part-00002-b9fda751-0b6f-4f60-ae2c-94c48b5bcb6b-c000.snappy.parquet
  part-00005-a9d642dd-0342-44c9-9a0d-f1cba095e34b-c000.snappy.parquet
  part-00007-73f043c4-1f01-4c08-b3aa-2230a48b60d4-c000.snappy.parquet

```

Delta Lake consists of a transaction log (\_delta\_log) and Parquet files in the filesystem.  
Let’s look at the contents of the first entry in the transaction log (00000000000000000000.json).

```
{
   "commitInfo":{
      "timestamp":1632491414394,
      "operation":"WRITE",
      "operationParameters":{
         "mode":"Overwrite",
         "partitionBy":"[]"
      },
      "isBlindAppend":false,
      "operationMetrics":{
         "numFiles":"4",
         "numOutputBytes":"2390",
         "numOutputRows":"3"
      }
   }
}{
   "protocol":{
      "minReaderVersion":1,
      "minWriterVersion":2
   }
}{
   "metaData":{
      "id":"db102a08-5265-4f86-a281-dfc8cccacf0e",
      "format":{
         "provider":"parquet",
         "options":{
            
         }
      },
      "schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"num\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
      "partitionColumns":[
         
      ],
      "configuration":{
         
      },
      "createdTime":1632491409910
   }
}{
   "add":{
      "path":"part-00000-193bf99f-66bf-4bbb-ab4c-868851bd5a24-c000.snappy.parquet",
      "partitionValues":{
         
      },
      "size":377,
      "modificationTime":1632491410000,
      "dataChange":true
   }
}{
   "add":{
      "path":"part-00002-b9fda751-0b6f-4f60-ae2c-94c48b5bcb6b-c000.snappy.parquet",
      "partitionValues":{
         
      },
      "size":674,
      "modificationTime":1632491410000,
      "dataChange":true
   }
}{
   "add":{
      "path":"part-00005-a9d642dd-0342-44c9-9a0d-f1cba095e34b-c000.snappy.parquet",
      "partitionValues":{
         
      },
      "size":654,
      "modificationTime":1632491410000,
      "dataChange":true
   }
}{
   "add":{
      "path":"part-00007-73f043c4-1f01-4c08-b3aa-2230a48b60d4-c000.snappy.parquet",
      "partitionValues":{
         
      },
      "size":685,
      "modificationTime":1632491410000,
      "dataChange":true
   }
}

```

The transaction log contains important information about the filesystem operations performed on the Delta Lake. This log entry tells us the following:

- Four Parquet files were added to the Delta Lake
- The sizes of all these Parquet files (one is empty)
- The schema of the Parquet file (so we can query the transaction log instead of opening Parquet files to figure out the schema)
- When these files were added to the lake, which gives us to opportunity to view the contents of the lake at different points in time

Let’s see how we can leverage delta-rs to time travel to different versions of the underlying Delta Lake with another example.

## Time travel

Let’s create another Delta Lake with two write transactions so we can demonstrate time travel.

Here’s the PySpark code with two different write transactions:

```python
data = [("a", 1), ("b", 2), ("c", 3)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.format("delta").save("resources/delta/2")

data = [("d", 4), ("e", 5), ("f", 6)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.mode("append").format("delta").save("resources/delta/2")

```

The first transaction wrote this data (version 0 of the dataset):

<table><tbody><tr><td><strong>letter</strong></td><td><strong>number</strong></td></tr><tr><td>a</td><td>1</td></tr><tr><td>b</td><td>2</td></tr><tr><td>c</td><td>3</td></tr></tbody></table>

The second transaction wrote this data (version 1 of the dataset):

<table><tbody><tr><td><strong>letter</strong></td><td><strong>number</strong></td></tr><tr><td>d</td><td>4</td></tr><tr><td>e</td><td>5</td></tr><tr><td>f</td><td>6</td></tr></tbody></table>

Read the entire dataset into a Dask DataFrame and print the contents.

```python
dt = DeltaTable("resources/delta/2")
filenames = ["resources/delta/2/" + f for f in dt.files()]

ddf = dd.read_parquet(filenames, engine="pyarrow")
print(ddf.compute())

  letter  number
0      d       4
0      a       1
0      e       5
0      b       2
0      c       3
0      f       6

```

Delta will grab the latest version of the dataset by default.

Now let’s time travel back to version 0 of the dataset and view the contents of the data before the second transaction was executed.

```python
dt = DeltaTable("resources/delta/2")
dt.load_version(0)
filenames = ["resources/delta/2/" + f for f in dt.files()]

ddf = dd.read_parquet(filenames, engine="pyarrow")
print(ddf.compute())

  letter  number
0      a       1
0      b       2
0      c       3

```

Delta Lake’s transaction log allows for out of the box time travel support.

Data scientists love the ability to time travel.  When models start giving different results, data scientists often struggle to understand why.  Are the model results different because the data lake changed or because the model code changed?  Or, what day was data added to the lake that caused the model to give different results?

With time travel, data scientists can train their model with different versions of the data and pinpoint exactly when the results changed.

Let’s look at another Delta Lake feature that helps when columns are added to the data.

## Schema evolution

Vanilla Parquet data lakes require that all files have the same schema.  If you add a Parquet file to a lake with a schema that doesn’t match all the existing files, the entire lake becomes corrupted and unreadable.

Suppose you have a lake with this data.

<table><tbody><tr><td><strong>letter</strong></td><td><strong>number</strong></td></tr><tr><td>a</td><td>1</td></tr><tr><td>b</td><td>2</td></tr><tr><td>c</td><td>3</td></tr></tbody></table>

You’d like to append this data to the lake:

<table><tbody><tr><td><strong>letter</strong></td><td><strong>number</strong></td><td><strong>color</strong></td></tr><tr><td>d</td><td>4</td><td>red</td></tr><tr><td>e</td><td>5</td><td>blue</td></tr><tr><td>f</td><td>6</td><td>green</td></tr></tbody></table>

Here’s how you can perform the initial write with PySpark.

```python
data = [("a", 1), ("b", 2), ("c", 3)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.format("delta").save("resources/delta/3")

```

Here’s how to append additional data with a different schema.

```python
data = [("d", 4, "red"), ("e", 5, "blue"), ("f", 6, "green")]
df = spark.createDataFrame(data, ["letter", "number", "color"])
df.write.mode("append").format("delta").save("resources/delta/3")

```

Read in the Delta Lake to a PySpark DataFrame and make sure it can gracefully handle the schema mismatch.

```python
df = spark.read.format("delta").option("mergeSchema", "true").load("resources/delta/3")
df.show()

+------+------+-----+                                                           
|letter|number|color|
+------+------+-----+
|     f|     6|green|
|     e|     5| blue|
|     d|     4|  red|
|     b|     2| null|
|     c|     3| null|
|     a|     1| null|
+------+------+-----+

```

Allowing schema mismatches saves you from having to rewrite your entire data lake every time you’d like to add a new column.

Delta Lake has lots of other schema options of course.  By default it’ll prevent data with mismatched schema from getting added to your lake.  You need to set the spark.databricks.delta.schema.autoMerge.enabled configuration option to True to allow for this schema merging behavior.

I’ll be reaching out to the core delta-rs team to figure out how to leverage schema evolution in Dask and Pandas.

## Conclusion

delta-rs makes it easy to read Delta Lakes into Dask DataFrames and leverage some Delta features like time travel.

Features are regularly being added to [delta-rs](https://github.com/delta-io/delta-rs) which will in turn be accessible by Dask.
