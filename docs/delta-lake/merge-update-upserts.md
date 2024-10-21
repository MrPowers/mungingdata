---
title: "Using Delta lake merge to update columns and perform upserts"
date: "2019-09-22"
categories: 
  - "delta-lake"
---

# Using Delta lake merge to update columns and perform upserts

This blog posts explains how to update a table column and perform upserts with the `merge` command.

We explain how to use the `merge` command and what the command does to the filesystem under the hood.

Parquet files are immutable, so `merge` provides an update-like interface, but doesn't actually mutate the underlying files. `merge` is slow on large datasets because Parquet files are immutable and the entire file needs to be rewritten, even if you only want to update a single column.

All of the examples in this post generally follow the examples in the [Delta update documentation](https://docs.delta.io/latest/delta-update.html).

## Merge example

Let's convert some CSV data into a Delta lake so we have some data to play with:

```
eventType,websitePage
click,homepage
clck,about page
mouseOver,logo
```

Here's the code to create the Delta lake:

```
val path = new java.io.File("./src/main/resources/event_data/").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .save(outputPath)
```

Let's take a look at what is stored in the `_delta_log/00000000000000000000.json` transaction log file.

```
{
  "add":{
    "path":"part-00000-f960ca7c-eff0-40d0-b753-1f99ea4ffb9f-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":673,
    "modificationTime":1569079218000,
    "dataChange":true
  }
}
```

Let's display the contents of our data lake:

```
val path = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
val df = spark.read.format("delta").load(path)
df.show()

+---------+-----------+
|eventType|websitePage|
+---------+-----------+
|    click|   homepage|
|     clck| about page|
|mouseOver|       logo|
+---------+-----------+
```

The second row of data has a typo in the eventType field. It says "clck" instead of "click".

Let's write a little code that'll update the typo.

```
val path = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
val deltaTable = DeltaTable.forPath(spark, path)

deltaTable.updateExpr(
  "eventType = 'clck'",
  Map("eventType" -> "'click'")
)

deltaTable.update(
  col("eventType") === "clck",
  Map("eventType" -> lit("click"))
)
```

We can check the contents of the Delta lake to confirm the spelling error has been fixed.

```
val path = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
val df = spark.read.format("delta").load(path)
df.show()

+---------+-----------+
|eventType|websitePage|
+---------+-----------+
|    click|   homepage|
|    click| about page|
|mouseOver|       logo|
+---------+-----------+
```

Parquet files are immutable… what does Delta do underneath the hood to fix the spelling error?

Let's take a look at the `_delta_log/00000000000000000001.json` to figure out what's going on underneath the hood.

```
{
  "remove":{
    "path":"part-00000-f960ca7c-eff0-40d0-b753-1f99ea4ffb9f-c000.snappy.parquet",
    "deletionTimestamp":1569079467662,
    "dataChange":true
  }
}

{
  "add":{
    "path":"part-00000-bcb431ea-f9d1-4399-9da5-3abfe5178d32-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":694,
    "modificationTime":1569079467000,
    "dataChange":true
  }
}
```

The merge command writes a new file to the filesystem. Let's inspect the contents of the new file.

```
val path = new java.io.File("./tmp/event_delta_lake/part-00000-bcb431ea-f9d1-4399-9da5-3abfe5178d32-c000.snappy.parquet").getCanonicalPath
val df = spark.read.parquet(path)
df.show()

+---------+-----------+
|eventType|websitePage|
+---------+-----------+
|    click|   homepage|
|    click| about page|
|mouseOver|       logo|
+---------+-----------+
```

Here are all the files in the filesystem after running the merge command.

```
event_delta_lake/
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
  part-00000-bcb431ea-f9d1-4399-9da5-3abfe5178d32-c000.snappy.parquet
  part-00000-f960ca7c-eff0-40d0-b753-1f99ea4ffb9f-c000.snappy.parquet
```

So the merge command is writing all the data in an entirely new file. It's unfortunately not able to go into the existing Parquet file and only update the cells that need to be changed.

Writing out all the data will make `merge` run a lot more slowly than you might expect.

## upsert example

Let's take the following data set and build another little Delta lake:

```
date,eventId,data
2019-01-01,4,take nap
2019-02-05,8,play smash brothers
2019-04-24,9,speak at spark summit
```

Here's the code to build the Delta lake:

```
val path = new java.io.File("./src/main/resources/upsert_event_data/original_data.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .save(outputPath)
```

Let's inspect the starting state of the Delta lake:

```
val path = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath
val df = spark.read.format("delta").load(path)
df.show(false)

+----------+-------+---------------------+
|date      |eventId|data                 |
+----------+-------+---------------------+
|2019-01-01|4      |take nap             |
|2019-02-05|8      |play smash brothers  |
|2019-04-24|9      |speak at spark summit|
+----------+-------+---------------------+
```

Let's update the Delta lake with a different phrasing of the events that's more "mom-friendly". We'll use this mom friendly data:

```
date,eventId,data
2019-01-01,4,set goals
2019-02-05,8,bond with nephew
2019-08-10,66,think about my mommy
```

Events 4 and 8 will be rephrased with descriptions that'll make mom proud. Event 66 will be added to the lake to make mom feel good.

Here's the code that'll perform the upsert:

```
val updatesPath = new java.io.File("./src/main/resources/upsert_event_data/mom_friendly_data.csv").getCanonicalPath
val updatesDF = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(updatesPath)

val path = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath

import io.delta.tables._

DeltaTable.forPath(spark, path)
  .as("events")
  .merge(
    updatesDF.as("updates"),
    "events.eventId = updates.eventId"
  )
  .whenMatched
  .updateExpr(
    Map("data" -> "updates.data")
  )
  .whenNotMatched
  .insertExpr(
    Map(
      "date" -> "updates.date",
      "eventId" -> "updates.eventId",
      "data" -> "updates.data")
  )
  .execute()
```

Let's take a look at the contents of the Delta lake after the upsert:

```
val path = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath
val df = spark.read.format("delta").load(path)
df.show(false)

+----------+-------+---------------------+
|date      |eventId|data                 |
+----------+-------+---------------------+
|2019-08-10|66     |think about my mommy |
|2019-04-24|9      |speak at spark summit|
|2019-02-05|8      |bond with nephew     |
|2019-01-01|4      |set goals            |
+----------+-------+---------------------+
```

## Transaction log for upserts

The `_delta_log/00000000000000000000.json` file contains a single entry for the single Parquet file that was added.

```
{
  "add":{
    "path":"part-00000-7eaa0d54-4dba-456a-ab80-b17f9aa7b583-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":900,
    "modificationTime":1569177685000,
    "dataChange":true
  }
}
```

The `_delta_log/00000000000000000001.json` file reveals that upserts surprisingly add a lot of records to the transaction log.

```
{
  "remove":{
    "path":"part-00000-7eaa0d54-4dba-456a-ab80-b17f9aa7b583-c000.snappy.parquet",
    "deletionTimestamp":1569177701037,
    "dataChange":true
  }
}

{
  "add":{
    "path":"part-00000-36aafda3-530d-4bd7-a29b-9c1716f18389-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":433,
    "modificationTime":1569177698000,
    "dataChange":true
  }
}

{
  "add":{
    "path":"part-00026-fcb37eb4-165f-4402-beb3-82d3d56bfe0c-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":968,
    "modificationTime":1569177700000,
    "dataChange":true
  }
}

{
  "add":{
    "path":"part-00139-eab3854f-4ed4-4856-8268-c89f0efe977c-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":1013,
    "modificationTime":1569177700000,
    "dataChange":true
  }
}

{
  "add":{
    "path":"part-00166-0e9cddc8-9104-4c11-8b7f-44a6441a95fb-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":905,
    "modificationTime":1569177700000,
    "dataChange":true
  }
}

{
  "add":{
    "path":"part-00178-147c78fa-dad2-4a1c-a4c5-65a1a647a41e-c000.snappy.parquet",
    "partitionValues":{

    },
    "size":1013,
    "modificationTime":1569177701000,
    "dataChange":true
  }
}
```

Let's create a little helper method that'll let us easily inspect the content of these parquet files:

```
def displayEventParquetFile(filename: String): Unit = {
  val path = new java.io.File(s"./tmp/upsert_event_delta_lake/$filename.snappy.parquet").getCanonicalPath
  val df = spark.read.parquet(path)
  df.show(false)
}

UpsertEventProcessor.displayEventParquetFile("part-00000-36aafda3-530d-4bd7-a29b-9c1716f18389-c000")

+----+-------+----+
|date|eventId|data|
+----+-------+----+
+----+-------+----+

UpsertEventProcessor.displayEventParquetFile("part-00026-fcb37eb4-165f-4402-beb3-82d3d56bfe0c-c000")

+----------+-------+----------------+
|date      |eventId|data            |
+----------+-------+----------------+
|2019-02-05|8      |bond with nephew|
+----------+-------+----------------+

UpsertEventProcessor.displayEventParquetFile("part-00139-eab3854f-4ed4-4856-8268-c89f0efe977c-c000")

+----------+-------+---------------------+
|date      |eventId|data                 |
+----------+-------+---------------------+
|2019-04-24|9      |speak at spark summit|
+----------+-------+---------------------+

UpsertEventProcessor.displayEventParquetFile("part-00166-0e9cddc8-9104-4c11-8b7f-44a6441a95fb-c000")

+----------+-------+---------+
|date      |eventId|data     |
+----------+-------+---------+
|2019-01-01|4      |set goals|
+----------+-------+---------+

UpsertEventProcessor.displayEventParquetFile("part-00178-147c78fa-dad2-4a1c-a4c5-65a1a647a41e-c000")

+----------+-------+--------------------+
|date      |eventId|data                |
+----------+-------+--------------------+
|2019-08-10|66     |think about my mommy|
+----------+-------+--------------------+
```

This update code creates a surprising number of Parquet files. Will need to test this code on a bigger dataset to see if this strangeness is intentional.

## Conclusion

Parquet files are immutable and don't support updates.

Delta lake provides `merge` statements to provide an update-like interface, but under the hood, these aren't real updates.

Delta lake is simply rewriting the entire Parquet files. This'll make an upsert or update column statement on a large dataset quite slow.
