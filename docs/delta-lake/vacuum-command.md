---
title: "Vacuuming Delta Lakes"
date: "2019-09-17"
categories: 
  - "delta-lake"
---

# Vacuuming Delta Lakes

Delta lakes are versioned so you can easily revert to old versions of the data.

In some instances, Delta lake needs to store multiple versions of the data to enable the rollback feature.

Storing multiple versions of the same data can get expensive, so Delta lake includes a `vacuum` command that deletes old versions of the data.

This blog post explains how to use the `vacuum` command and situations where it is applicable.

## Simple example

Let's use the following CSV file to make a Delta lake.

```
first_name,last_name,country
miguel,cordoba,colombia
luisa,gomez,colombia
li,li,china
wang,wei,china
hans,meyer,germany
mia,schmidt,germany
```

Here's the code to create the Delta lake.

```
val path = new java.io.File("./src/main/resources/person_data/people1.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .save(outputPath)
```

Let's view the content of the Delta lake:

```
val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
val df = spark.read.format("delta").load(path)
df.show()

+----------+---------+--------+
|first_name|last_name| country|
+----------+---------+--------+
|    miguel|  cordoba|colombia|
|     luisa|    gomez|colombia|
|        li|       li|   china|
|      wang|      wei|   china|
|      hans|    meyer| germany|
|       mia|  schmidt| germany|
+----------+---------+--------+
```

Here are the contents of the filesystem after the first write:

```
vacuum_example/
  _delta_log/
    00000000000000000000.json
  part-00000-d7ec54f9-eee2-40ae-b8b9-a6786214d3ac-c000.snappy.parquet
```

Let's overwrite the data in the Delta lake with another CSV file:

```
first_name,last_name,country
lou,bega,germany
bradley,nowell,usa
```

Here's the code that'll overwrite the lake:

```
val path = new java.io.File("./src/main/resources/person_data/people2.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save(outputPath)
```

Here's what the data looks like after the overwrite:

```
val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
val df = spark.read.format("delta").load(path)
df.show()

+----------+---------+-------+
|first_name|last_name|country|
+----------+---------+-------+
|       lou|     bega|germany|
|   bradley|   nowell|    usa|
+----------+---------+-------+
```

Here are the contents of the filesystem after the second write:

```
vacuum_example/
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
  part-00000-4c44ec8c-6d02-4f01-91f7-78c05df0fd27-c000.snappy.parquet
  part-00000-d7ec54f9-eee2-40ae-b8b9-a6786214d3ac-c000.snappy.parquet
```

So the data from the first write isn't read into our DataFrame anymore, but it's still stored in the filesystem.

This allows us to rollback to an older version of the data.

Let's display the contents of our Delta lake as of version 0:

```
val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
val df = spark.read.format("delta").option("versionAsOf", 0).load(path)
df.show()

+----------+---------+--------+
|first_name|last_name| country|
+----------+---------+--------+
|    miguel|  cordoba|colombia|
|     luisa|    gomez|colombia|
|        li|       li|   china|
|      wang|      wei|   china|
|      hans|    meyer| germany|
|       mia|  schmidt| germany|
+----------+---------+--------+
```

Delta lake provides a vacuum command that deletes older versions of the data (any data that's older than the specified retention period).

Let's run the `vacuum` command and verify the file is deleted in the filesystem.

```
val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
import io.delta.tables._
val deltaTable = DeltaTable.forPath(spark, path)
deltaTable.vacuum(0.000001)
```

We set the retention period to 0.000001 hours so we can run this vacuum command right away.

Here's what the filesystem looks like after running the `vacuum` command.

```
vacuum_example/
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
  part-00000-4c44ec8c-6d02-4f01-91f7-78c05df0fd27-c000.snappy.parquet
```

Let's look at the `00000000000000000001.json` file to understand how Delta knows what files to delete:

```
{
  "add":{
    "path":"part-00000-4c44ec8c-6d02-4f01-91f7-78c05df0fd27-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":832,
    "modificationTime":1568680993000,
    "dataChange":true
  }
}

{
  "remove":{
    "path":"part-00000-d7ec54f9-eee2-40ae-b8b9-a6786214d3ac-c000.snappy.parquet",
    "deletionTimestamp":1568680995519,
    "dataChange":true
  }
}
```

The `remove` part of the JSON file indicates that `part-00000-d7ec54f9-....snappy.parquet` can be deleted when the `vacuum` command is run.

We cannot access version 0 of the Delta lake after the vacuum command has been run:

```
val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
val df = spark.read.format("delta").option("versionAsOf", 0).load(path)
df.show()

org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1645.0 failed 1 times, most recent failure: Lost task 0.0 in stage 1645.0 (TID 21123, localhost, executor driver): java.io.FileNotFoundException: File file:/Users/powers/Documents/code/my_apps/yello-taxi/tmp/vacuum_example/part-00000-618d3c69-3d1b-402c-818f-9d3995b5639f-c000.snappy.parquet does not exist
It is possible the underlying files have been updated. You can explicitly invalidate the cache in Spark by running 'REFRESH TABLE tableName' command in SQL or by recreating the Dataset/DataFrame involved.
Cause: java.io.FileNotFoundException: File file:/Users/powers/Documents/code/my_apps/yello-taxi/tmp/vacuum_example/part-00000-618d3c69-3d1b-402c-818f-9d3995b5639f-c000.snappy.parquet does not exist
It is possible the underlying files have been updated. You can explicitly invalidate the cache in Spark by running 'REFRESH TABLE tableName' command in SQL or by recreating the Dataset/DataFrame involved.
```

## Default retention period

The retention period is 7 days by default.

So `deltaTable.vacuum()` wouldn't do anything unless we waited 7 days to run the command.

You need to set a special command to invoke the `vacuum` method with a retention period that's less than 7 days. Otherwise you'll get the following error message:

```
java.lang.IllegalArgumentException: requirement failed: Are you sure you would like to vacuum files with such a low retention period? If you have
writers that are currently writing to this table, there is a risk that you may corrupt the
state of your Delta table.

If you are certain that there are no operations being performed on this table, such as
insert/upsert/delete/optimize, then you may turn off this check by setting:
spark.databricks.delta.retentionDurationCheck.enabled = false

If you are not sure, please use a value not less than "168 hours".
```

We need to update the Spark configuration to allow for such a short retention period.

```
lazy val spark: SparkSession = {
  SparkSession
    .builder()
    .master("local")
    .appName("spark session")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()
}
```

## When vacuum does nothing

Let's look at another example where we're simply adding data to the lake, so running the vacuum command won't do anything.

Here's the `dogs1.csv` file:

```
first_name,breed
fido,lab
spot,bulldog
```

And here's the `dogs2.csv` file:

```
first_name,breed
fido,beagle
lou,pug
```

Here's some code to write out `dog1.csv` and `dogs2.csv` as Delta lake files.

```
val path = new java.io.File("./src/main/resources/dog_data/dogs1.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/vacuum_example2/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .save(outputPath)

val path2 = new java.io.File("./src/main/resources/dog_data/dogs2.csv").getCanonicalPath
val df2 = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path2)

df2
  .repartition(1)
  .write
  .format("delta")
  .mode(SaveMode.Append)
  .save(outputPath)
```

Here's what the Delta lake contains after both files are written:

```
+----------+-------+
|first_name|  breed|
+----------+-------+
|      fido|    lab|
|      spot|bulldog|
|      fido| beagle|
|       lou|    pug|
+----------+-------+
```

This is what the filesystem looks like:

```
vacuum_example2/
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
  part-00000-57db2297-9aaf-44b6-b940-48c504c510d1-c000.snappy.parquet
  part-00000-6574b35c-677b-4423-95ae-993638f222cf-c000.snappy.parquet
```

Here are the contents of the `00000000000000000000.json` file:

```
{
  "add":{
    "path":"part-00000-57db2297-9aaf-44b6-b940-48c504c510d1-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":606,
    "modificationTime":1568685380000,
    "dataChange":true
  }
}
```

Here are the contents of the `00000000000000000001.json` file.

```
{
  "add":{
    "path":"part-00000-6574b35c-677b-4423-95ae-993638f222cf-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":600,
    "modificationTime":1568685386000,
    "dataChange":true
  }
}
```

None of the JSON files contain any `remove` lines so a `vacuum` won't delete any files.

This code doesn't change anything:

```
val path = new java.io.File("./tmp/vacuum_example2/").getCanonicalPath
import io.delta.tables._
val deltaTable = DeltaTable.forPath(spark, path)
deltaTable.vacuum(0.000001)
```

## Conclusion

Use `vacuum()` to delete files from your Delta lake if you'd like to save on data storage costs.

You'll often have duplicate files after running `Overwrite` operations. Any files that are older than the specified retention period and are marked as `remove` in the `_delta_log/` JSON files will be deleted when `vacuum` is run.
