---
title: "Type 2 Slowly Changing Dimension Upserts with Delta Lake"
date: "2021-01-30"
categories: 
  - "delta-lake"
---

# Type 2 Slowly Changing Dimension Upserts with Delta Lake

This post explains how to perform type 2 upserts for slowly changing dimension tables with Delta Lake.

We'll start out by covering the basics of type 2 SCDs and when they're advantageous.

This post is [inspired by the Databricks docs](https://docs.delta.io/0.4.0/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables), but contains significant modifications and more context so the example is easier to follow.

Delta lake upserts are challenging. You'll need to study this post carefully.

## Type 2 SCD basics

Take a look at the following data on tech celebrities:

```
+--------+----------+------------+-----------+---------+-------------+----------+
|personId|personName|     country|     region|isCurrent|effectiveDate|   endDate|
+--------+----------+------------+-----------+---------+-------------+----------+
|       1| elon musk|south africa|   pretoria|     true|   1971-06-28|      null|
|       2|jeff bezos|          us|albuquerque|     true|   1964-01-12|      null|
|       3|bill gates|          us|    seattle|    false|   1955-10-28|1973-09-01|
+--------+----------+------------+-----------+---------+-------------+----------+
```

`personId`, `personName`, `country`, and `region` are traditional database columns.

`isCurrent`, `effectiveDate`, and `endDate` are columns to make this a type 2 SCD table.

The type 2 SCD fields let you see the history of your data, not just the current state.

It lets you run queries to find where Elon Musk currently currently lives and also where he lived in May 1993.

## Upserts

Suppose you'd like to update your table with the following data:

```
+--------+----------+-------+--------+-------------+
|personId|personName|country|  region|effectiveDate|
+--------+----------+-------+--------+-------------+
|       1| elon musk| canada|montreal|   1989-06-01|
|       4|       dhh|     us| chicago|   2005-11-01|
+--------+----------+-------+--------+-------------+
```

Elon Musk is in the original data table. DHH is not in the original table.

You'd like to update the Elon Musk record and insert a new row for DHH. These update/insert operations are referred to as upserts.

Here's the end table we'd like to create:

```
+--------+----------+------------+-----------+---------+-------------+----------+
|personId|personName|     country|     region|isCurrent|effectiveDate|   endDate|
+--------+----------+------------+-----------+---------+-------------+----------+
|       1| elon musk|south africa|   pretoria|    false|   1971-06-28|1989-06-01|
|       3|bill gates|          us|    seattle|    false|   1955-10-28|1973-09-01|
|       1| elon musk|      canada|   montreal|     true|   1989-06-01|      null|
|       2|jeff bezos|          us|albuquerque|     true|   1964-01-12|      null|
|       4|       dhh|          us|    chicago|     true|   2005-11-01|      null|
+--------+----------+------------+-----------+---------+-------------+----------+
```

The Elon Musk South Africa row was updated to reflect that he lived there from 1971 till 1989. A new row was added for when Elon moved to Canada. We'd need to add additional rows for when he moved to California and Texas to make this table complete, but you get the idea.

A new row was added for DHH. There wasn't any existing data for DHH, so this was a plain insert.

The rest of this post explains how to make this upsert.

## Create Delta table

Let's create a Delta table with our example data:

```
val df = Seq(
  (1, "elon musk", "south africa", "pretoria", true, "1971-06-28", null),
  (2, "jeff bezos", "us", "albuquerque", true, "1964-01-12", null),
  (3, "bill gates", "us", "seattle", false, "1955-10-28", "1973-09-01")
).toDF("personId", "personName", "country", "region", "isCurrent", "effectiveDate", "endDate")

val path = os.pwd/"tmp"/"tech_celebs"

df
  .write
  .format("delta")
  .mode("overwrite")
  .save(path.toString())

df.show()
```

```
+--------+----------+------------+-----------+---------+-------------+----------+
|personId|personName|     country|     region|isCurrent|effectiveDate|   endDate|
+--------+----------+------------+-----------+---------+-------------+----------+
|       1| elon musk|south africa|   pretoria|     true|   1971-06-28|      null|
|       2|jeff bezos|          us|albuquerque|     true|   1964-01-12|      null|
|       3|bill gates|          us|    seattle|    false|   1955-10-28|1973-09-01|
+--------+----------+------------+-----------+---------+-------------+----------+
```

The elegant `os.pwd` syntax is powered by [os-lib](https://github.com/lihaoyi/os-lib), [as explained in this post](https://mungingdata.com/scala/filesystem-paths-move-copy-list-delete-folders/).

## Build update table

Create another DataFrame with the update data so we can perform an upsert.

```
val updatesDF = Seq(
  (1, "elon musk", "canada", "montreal", "1989-06-01"),
  (4, "dhh", "us", "chicago", "2005-11-01")
).toDF("personId", "personName", "country", "region", "effectiveDate")

updatesDF.show()
```

```
+--------+----------+-------+--------+-------------+
|personId|personName|country|  region|effectiveDate|
+--------+----------+-------+--------+-------------+
|       1| elon musk| canada|montreal|   1989-06-01|
|       4|       dhh|     us| chicago|   2005-11-01|
+--------+----------+-------+--------+-------------+
```

## Build staged update table

We'll need to modify the update table, so it's properly formatted for the upsert. We need three rows in the staged upsert table:

- Elon Musk update South Africa row
- Elon Must insert Canada row
- DHH insert Chicago row

Delta uses Parquet files, which are immutable, so updates aren't performed in the traditional sense. Updates are really deletes then inserts. Study up on [Delta basics](https://mungingdata.com/category/delta-lake/) if you're new to the technology.

Let's compute the rows for existing people in the data that have new data. This is the existing Elon Musk row. Notice that the `mergeKey` is intentionally set to `null` in the following code.

```
val stagedPart1 = updatesDF
  .as("updates")
  .join(techCelebsTable.toDF.as("tech_celebs"), "personId")
  .where("tech_celebs.isCurrent = true AND (updates.country <> tech_celebs.country OR updates.region <> tech_celebs.region)")
  .selectExpr("NULL as mergeKey", "updates.*")

stagedPart1.show()
```

```
+--------+--------+----------+-------+--------+-------------+
|mergeKey|personId|personName|country|  region|effectiveDate|
+--------+--------+----------+-------+--------+-------------+
|    null|       1| elon musk| canada|montreal|   1989-06-01|
+--------+--------+----------+-------+--------+-------------+
```

Here are the other rows that need to be inserted.

```
val stagedPart2 = updatesDF.selectExpr("personId as mergeKey", "*")

stagedPart2.show()
```

```
+--------+--------+----------+-------+--------+-------------+
|mergeKey|personId|personName|country|  region|effectiveDate|
+--------+--------+----------+-------+--------+-------------+
|       1|       1| elon musk| canada|montreal|   1989-06-01|
|       4|       4|       dhh|     us| chicago|   2005-11-01|
+--------+--------+----------+-------+--------+-------------+
```

Create the staged update table by unioning the two DataFrames.

```
val stagedUpdates = stagedPart1.union(stagedPart2)

stagedUpdates.show()
```

```
+--------+--------+----------+-------+--------+-------------+
|mergeKey|personId|personName|country|  region|effectiveDate|
+--------+--------+----------+-------+--------+-------------+
|    null|       1| elon musk| canada|montreal|   1989-06-01|
|       1|       1| elon musk| canada|montreal|   1989-06-01|
|       4|       4|       dhh|     us| chicago|   2005-11-01|
+--------+--------+----------+-------+--------+-------------+
```

The two Elon Musk rows in the staged upsert table are important. We need both, one with the `mergeKey` set to `null` and another with a populated `mergeKey` value.

We're ready to perform the upsert now that the staged upsert table is properly formatted.

## Perform the upsert

Delta exposes an elegant Scala DSL for performing upserts.

```
techCelebsTable
  .as("tech_celebs")
  .merge(stagedUpdates.as("staged_updates"), "tech_celebs.personId = mergeKey")
  .whenMatched("tech_celebs.isCurrent = true AND (staged_updates.country <> tech_celebs.country OR staged_updates.region <> tech_celebs.region)")
  .updateExpr(Map(
    "isCurrent" -> "false",
    "endDate" -> "staged_updates.effectiveDate"))
  .whenNotMatched()
  .insertExpr(Map(
    "personId" -> "staged_updates.personId",
    "personName" -> "staged_updates.personName",
    "country" -> "staged_updates.country",
    "region" -> "staged_updates.region",
    "isCurrent" -> "true",
    "effectiveDate" -> "staged_updates.effectiveDate",
    "endDate" -> "null"))
  .execute()

val resDF =  spark
  .read
  .format("delta")
  .load(path.toString())

resDF.show()
```

```
+--------+----------+------------+-----------+---------+-------------+----------+
|personId|personName|     country|     region|isCurrent|effectiveDate|   endDate|
+--------+----------+------------+-----------+---------+-------------+----------+
|       1| elon musk|south africa|   pretoria|    false|   1971-06-28|1989-06-01|
|       3|bill gates|          us|    seattle|    false|   1955-10-28|1973-09-01|
|       1| elon musk|      canada|   montreal|     true|   1989-06-01|      null|
|       2|jeff bezos|          us|albuquerque|     true|   1964-01-12|      null|
|       4|       dhh|          us|    chicago|     true|   2005-11-01|      null|
+--------+----------+------------+-----------+---------+-------------+----------+
```

Remember that Elon Musk has two rows in the staging table:

- One with a `mergeKey` of `null`
- Another with a `mergeKey` of 1

When the `mergeKey` is 1, then the row is considered matched, and only the `isCurrent` and `endDate` fields are updated. That's how the Elon Musk South Africa row is updated.

The Elon Musk Canada and DHH rows are considered "not matched" and are inserted with different logic.

## Tying everything together

Study the initial table, the staged update table, and the final result side-by-side to understand the result:

```
+--------+----------+------------+-----------+---------+-------------+----------+
|personId|personName|     country|     region|isCurrent|effectiveDate|   endDate|
+--------+----------+------------+-----------+---------+-------------+----------+
|       1| elon musk|south africa|   pretoria|     true|   1971-06-28|      null|
|       2|jeff bezos|          us|albuquerque|     true|   1964-01-12|      null|
|       3|bill gates|          us|    seattle|    false|   1955-10-28|1973-09-01|
+--------+----------+------------+-----------+---------+-------------+----------+

+--------+--------+----------+-------+--------+-------------+
|mergeKey|personId|personName|country|  region|effectiveDate|
+--------+--------+----------+-------+--------+-------------+
|    null|       1| elon musk| canada|montreal|   1989-06-01| NOT matched
|       1|       1| elon musk| canada|montreal|   1989-06-01| matched
|       4|       4|       dhh|     us| chicago|   2005-11-01| NOT matched
+--------+--------+----------+-------+--------+-------------+

+--------+----------+------------+-----------+---------+-------------+----------+
|personId|personName|     country|     region|isCurrent|effectiveDate|   endDate|
+--------+----------+------------+-----------+---------+-------------+----------+
|       1| elon musk|south africa|   pretoria|    false|   1971-06-28|1989-06-01|
|       3|bill gates|          us|    seattle|    false|   1955-10-28|1973-09-01|
|       1| elon musk|      canada|   montreal|     true|   1989-06-01|      null|
|       2|jeff bezos|          us|albuquerque|     true|   1964-01-12|      null|
|       4|       dhh|          us|    chicago|     true|   2005-11-01|      null|
+--------+----------+------------+-----------+---------+-------------+----------+
```

## Inspect the filesystem output

The Delta transaction log has two entries. The first transaction adds a single Parquet file: `part-00000-2cc6a8d9-86ee-4292-a850-9f5e01918c0d-c000.snappy.parquet`.

The second transaction performed these filesystem operations:

- remove file `part-00000-2cc6a8d9-86ee-4292-a850-9f5e01918c0d-c000.snappy.parquet`
- add file `part-00000-daa6c389-2894-4a6b-a012-618e830574c6-c000.snappy.parquet`
- add file `part-00042-d38c2d50-7910-4658-b297-84c51cf4b196-c000.snappy.parquet`
- add file `part-00043-acda103d-c5d7-4062-a6f8-0a112f4425f7-c000.snappy.parquet`
- add file `part-00051-c5d71959-a214-44f9-97fc-a0b928a19393-c000.snappy.parquet`
- add file `part-00102-41b139c3-a6cd-4a1a-814d-f509b84459a9-c000.snappy.parquet`
- add file `part-00174-693e1b33-cbd1-4d93-b4fc-fbf4c7f00878-c000.snappy.parquet`

Here's the contents of all the files in the second transaction.

part-00000-2cc6a8d9-86ee-4292-a850-9f5e01918c0d-c000.snappy.parquet (removed):

```
+--------+----------+------------+-----------+---------+-------------+----------+
|personId|personName|     country|     region|isCurrent|effectiveDate|   endDate|
+--------+----------+------------+-----------+---------+-------------+----------+
|       1| elon musk|south africa|   pretoria|     true|   1971-06-28|      null|
|       2|jeff bezos|          us|albuquerque|     true|   1964-01-12|      null|
|       3|bill gates|          us|    seattle|    false|   1955-10-28|1973-09-01|
+--------+----------+------------+-----------+---------+-------------+----------+
```

part-00000-daa6c389-2894-4a6b-a012-618e830574c6-c000.snappy.parquet (not sure why an empty file is added):

```
+--------+----------+-------+------+---------+-------------+-------+
|personId|personName|country|region|isCurrent|effectiveDate|endDate|
+--------+----------+-------+------+---------+-------------+-------+
+--------+----------+-------+------+---------+-------------+-------+
```

part-00042-d38c2d50-7910-4658-b297-84c51cf4b196-c000.snappy.parquet (added):

```
+--------+----------+-------+--------+---------+-------------+-------+
|personId|personName|country|  region|isCurrent|effectiveDate|endDate|
+--------+----------+-------+--------+---------+-------------+-------+
|       1| elon musk| canada|montreal|     true|   1989-06-01|   null|
+--------+----------+-------+--------+---------+-------------+-------+
```

part-00043-acda103d-c5d7-4062-a6f8-0a112f4425f7-c000.snappy.parquet (added):

```
+--------+----------+------------+--------+---------+-------------+----------+
|personId|personName|     country|  region|isCurrent|effectiveDate|   endDate|
+--------+----------+------------+--------+---------+-------------+----------+
|       1| elon musk|south africa|pretoria|    false|   1971-06-28|1989-06-01|
+--------+----------+------------+--------+---------+-------------+----------+
```

part-00051-c5d71959-a214-44f9-97fc-a0b928a19393-c000.snappy.parquet (added):

```
+--------+----------+-------+-------+---------+-------------+----------+
|personId|personName|country| region|isCurrent|effectiveDate|   endDate|
+--------+----------+-------+-------+---------+-------------+----------+
|       3|bill gates|     us|seattle|    false|   1955-10-28|1973-09-01|
+--------+----------+-------+-------+---------+-------------+----------+
```

part-00102-41b139c3-a6cd-4a1a-814d-f509b84459a9-c000.snappy.parquet (added):

```
+--------+----------+-------+-------+---------+-------------+-------+
|personId|personName|country| region|isCurrent|effectiveDate|endDate|
+--------+----------+-------+-------+---------+-------------+-------+
|       4|       dhh|     us|chicago|     true|   2005-11-01|   null|
+--------+----------+-------+-------+---------+-------------+-------+
```

part-00174-693e1b33-cbd1-4d93-b4fc-fbf4c7f00878-c000.snappy.parquet (added):

```
+--------+----------+-------+-----------+---------+-------------+-------+
|personId|personName|country|     region|isCurrent|effectiveDate|endDate|
+--------+----------+-------+-----------+---------+-------------+-------+
|       2|jeff bezos|     us|albuquerque|     true|   1964-01-12|   null|
+--------+----------+-------+-----------+---------+-------------+-------+
```

Adding single row Parquet files seems silly, but Delta isn't optimized to run on tiny datasets.

Delta is powerful because it can perform these upserts on huge datasets.

## Next steps

Watch [the Databricks talk on type 2 SCDs](https://www.youtube.com/watch?v=HZWwZG07hzQ&t&ab_channel=Databricks) and [Dominique's excellent presentation](https://youtu.be/vqMuECdmXG0) on working with Delta Lake at a massive scale.

See [this commit](https://github.com/MrPowers/delta-examples/commit/1705831d6218b563e043c127da67ef3986a26dee) for the code covered in this post. You can clone the repo, run this code on your local machine, and observe the files that are created. That's a great way to learn.

Check out the other [Data Lake posts](https://mungingdata.com/category/delta-lake/) for topics that aren't quite as advanced as upserts. If you're new to Delta Lake, it's best to master the introductory concepts like [the basics of the transaction log and time travel](https://mungingdata.com/delta-lake/introduction-time-travel/), before moving to more advanced concepts.
