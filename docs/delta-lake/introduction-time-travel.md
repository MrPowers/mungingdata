---
title: "Introduction to Delta Lake and Time Travel"
date: "2019-08-08"
categories: 
  - "delta-lake"
---

# Introduction to Delta Lake and Time Travel

[Delta Lake](https://delta.io/) is a wonderful technology that adds powerful features to Parquet data lakes.

This blog post demonstrates how to create and incrementally update Delta lakes.

We will learn how the Delta transaction log stores data lake metadata.

Then we'll see how the transaction log allows us to time travel and explore our data at a given point in time.

## Creating a Delta data lake

Let's create a Delta lake from a CSV file with data on people. Here's the CSV data we'll use:

```
first_name,last_name,country
miguel,cordoba,colombia
luisa,gomez,colombia
li,li,china
wang,wei,china
hans,meyer,germany
mia,schmidt,germany
```

Here's the code that'll read the CSV file into a DataFrame and write it out as a Delta data lake (all of the code in this post in stored in [this GitHub repo](https://github.com/MrPowers/yellow-taxi)).

```scala
val path = new java.io.File("./src/main/resources/person_data/").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/person_delta_lake/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save(outputPath)
```

The `person_data_lake` directory will contain these files:

```
person_data_lake/
  part-00000-78f9c583-ea60-4962-af99-895f453dce23-c000.snappy.parquet
  _delta_log/
    00000000000000000000.json
```

The data is stored in a Parquet file and the metadata is stored in the `_delta_log/00000000000000000000.json` file.

The JSON file contains information on the write transaction, schema of the data, and what file was added. Let's inspect the contents of the JSON file.

```
{
  "commitInfo":{
    "timestamp":1565119301357,
    "operation":"WRITE",
    "operationParameters":{
      "mode":"Overwrite",
      "partitionBy":"[]"
    }
  }
}{
  "protocol":{
    "minReaderVersion":1,
    "minWriterVersion":2
  }
}{
  "metaData":{
    "id":"a3ca108e-3ba1-49dc-99a0-c9d29c8f1aec",
    "format":{
      "provider":"parquet",
      "options":{

      }
    },
    "schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"last_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"country\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns":[

    ],
    "configuration":{

    },
    "createdTime":1565119298882
  }
}{
  "add":{
    "path":"part-00000-78f9c583-ea60-4962-af99-895f453dce23-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":939,
    "modificationTime":1565119299000,
    "dataChange":true
  }
}
```

## Incrementally updating Delta data lake

Let's use some [New York City taxi data](https://data.cityofnewyork.us/Transportation/2018-Yellow-Taxi-Trip-Data/t29m-gskq) to build and then incrementally update a Delta data lake.

Here's the code that'll initially build the Delta data lake:

```scala
val outputPath = new java.io.File("./tmp/incremental_delta_lake/").getCanonicalPath

val p1 = new java.io.File("./src/main/resources/taxi_data/taxi1.csv").getCanonicalPath
val df1 = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(p1)
df1
  .repartition(1)
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save(outputPath)
```

This code creates a Parquet file and a `_delta_log/00000000000000000000.json` file.

```
incremental_delta_lake/
  part-00000-b38c0ad6-2e36-47a3-baa1-3f339950f931-c000.snappy.parquet
  _delta_log/
    00000000000000000000.json
```

Let's inspect the contents of the incremental Delta data lake.

```scala
spark
  .read
  .format("delta")
  .load(outputPath)
  .select("passenger_count", "fare_amount")
  .show()

+---------------+-----------+
|passenger_count|fare_amount|
+---------------+-----------+
|              2|          4|
|              1|        4.5|
|              4|         12|
|              2|       10.5|
|              1|          5|
+---------------+-----------+
```

The Delta lake contains 5 rows of data after the first load.

Let's load another file into the Delta data lake with `SaveMode.Append`:

```scala
val p2 = new java.io.File("./src/main/resources/taxi_data/taxi2.csv").getCanonicalPath
val df2 = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(p2)
df2
  .repartition(1)
  .write
  .format("delta")
  .mode(SaveMode.Append)
  .save(outputPath)
```

This code creates a Parquet file and a `_delta_log/00000000000000000001.json` file. The `incremental_data_lake` contains these files now:

```
incremental_delta_lake/
  part-00000-b38c0ad6-2e36-47a3-baa1-3f339950f931-c000.snappy.parquet
  part-00000-fda221a5-1ec6-4320-bd1d-e767f7ee4799-c000.snappy.parquet
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
```

The Delta lake contains 10 rows of data after the file is loaded:

```scala
spark
  .read
  .format("delta")
  .load(outputPath)
  .select("passenger_count", "fare_amount")
  .show()

+---------------+-----------+
|passenger_count|fare_amount|
+---------------+-----------+
|              2|         52|
|              3|       43.5|
|              2|       24.5|
|              1|         52|
|              1|          4|
|              2|          4|
|              1|        4.5|
|              4|         12|
|              2|       10.5|
|              1|          5|
+---------------+-----------+
```

## Time travel

Delta lets you time travel and explore the state of the data lake as of a given data load. Let's write a query to examine the incrementally updating Delta data lake after the first data load (ignoring the second data load).

```scala
spark
  .read
  .format("delta")
  .option("versionAsOf", 0)
  .load(outputPath)
  .select("passenger_count", "fare_amount")
  .show()

+---------------+-----------+
|passenger_count|fare_amount|
+---------------+-----------+
|              2|          4|
|              1|        4.5|
|              4|         12|
|              2|       10.5|
|              1|          5|
+---------------+-----------+
```

The `option("versionAsOf", 0)` tells Delta to only grab the files in `_delta_log/00000000000000000000.json` and ignore the files in `_delta_log/00000000000000000001.json`.

Let's say you're training a machine learning model off of a data lake and want to hold the data constant while experimenting. Delta lake makes it easy to use a single version of the data when you're training your model.

You can easily access a full history of the Delta lake transaction log.

```scala
import io.delta.tables._

val lakePath = new java.io.File("./tmp/incremental_delta_lake/").getCanonicalPath
val deltaTable = DeltaTable.forPath(spark, lakePath)
val fullHistoryDF = deltaTable.history()
fullHistoryDF.show()
```

```
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+
|version|          timestamp|userId|userName|operation| operationParameters| job|notebook|clusterId|readVersion|isolationLevel|isBlindAppend|
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+
|      2|2019-08-15 16:55:51|  null|    null|    WRITE|[mode -> Append, ...|null|    null|     null|          1|          null|         true|
|      1|2019-08-15 16:55:38|  null|    null|    WRITE|[mode -> Append, ...|null|    null|     null|          0|          null|         true|
|      0|2019-08-15 16:55:29|  null|    null|    WRITE|[mode -> Overwrit...|null|    null|     null|       null|          null|        false|
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+
```

The schema of the Delta history table is as follows:

```
fullHistoryDF.printSchema()

root
 |-- version: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- userId: string (nullable = true)
 |-- userName: string (nullable = true)
 |-- operation: string (nullable = true)
 |-- operationParameters: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- job: struct (nullable = true)
 |    |-- jobId: string (nullable = true)
 |    |-- jobName: string (nullable = true)
 |    |-- runId: string (nullable = true)
 |    |-- jobOwnerId: string (nullable = true)
 |    |-- triggerType: string (nullable = true)
 |-- notebook: struct (nullable = true)
 |    |-- notebookId: string (nullable = true)
 |-- clusterId: string (nullable = true)
 |-- readVersion: long (nullable = true)
 |-- isolationLevel: string (nullable = true)
 |-- isBlindAppend: boolean (nullable = true)
```

We can also grab a Delta table version by timestamp.

```scala
val lakePath = new java.io.File("./tmp/incremental_delta_lake/").getCanonicalPath
spark
  .read
  .format("delta")
  .option("timestampAsOf", "2019-08-15 16:55:38")
  .load(lakePath)
```

This is the same as grabbing version 1 of our Delta table (examine the transaction log history output to see why):

```scala
val lakePath = new java.io.File("./tmp/incremental_delta_lake/").getCanonicalPath
spark
  .read
  .format("delta")
  .option("versionAsOf", 1)
  .load(lakePath)
  .show()
```

## Next steps

This blog post just scratches the surface on the host of features offered by Delta Lake.

In the coming blog posts we'll explore how to compact Delta lakes, schema evolution, schema enforcement, updates, deletes, and streaming.
