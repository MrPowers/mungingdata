---
title: "Selectively updating Delta partitions with replaceWhere"
date: "2019-10-23"
categories: 
  - "delta-lake"
---

# Selectively updating Delta partitions with replaceWhere

Delta makes it easy to update certain disk partitions with the `replaceWhere` option.

Selectively applying updates to certain partitions isn't always possible (sometimes the entire lake needs the update), but can result in significant speed gains.

Let's start with a simple example and then explore situations where the `replaceWhere` update pattern is applicable.

## Simple example

Suppose we have the following five rows of data in a CSV file:

```
first_name,last_name,country
Ernesto,Guevara,Argentina
Vladimir,Putin,Russia
Maria,Sharapova,Russia
Bruce,Lee,China
Jack,Ma,China
```

Let's create a Delta lake from the CSV file:

```
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)
  .withColumn("continent", lit(null).cast(StringType))

val deltaPath = new java.io.File("./tmp/country_partitioned_lake/").getCanonicalPath

df
  .repartition(col("country"))
  .write
  .partitionBy("country")
  .format("delta")
  .mode("overwrite")
  .save(deltaPath)
```

We're appending a blank `continent` column to the DataFrame before writing it out as a Delta table, so we won't have any schema mismatch issues.

Let's define a custom DataFrame transformation that'll append a `continent` column to a DataFrame:

```
def withContinent()(df: DataFrame): DataFrame = {
  df.withColumn(
    "continent",
    when(col("country") === "Russia", "Europe")
      .when(col("country") === "China", "Asia")
      .when(col("country") === "Argentina", "South America")
  )
}
```

Suppose the business would like us to populate the `continent` column, but only for the China partition. We can use `replaceWhere` to only update the China partition.

```
spark.read.format("delta").load(deltaPath)
  .where(col("country") === "China")
  .transform(withContinent())
  .write
  .format("delta")
  .option("replaceWhere", "country = 'China'")
  .mode("overwrite")
  .save(deltaPath)
```

Let's view the contents of the Delta lake:

```
spark
  .read
  .format("delta")
  .load(deltaPath)
  .show(false)

+----------+---------+---------+---------+
|first_name|last_name|country  |continent|
+----------+---------+---------+---------+
|Ernesto   |Guevara  |Argentina|null     |
|Bruce     |Lee      |China    |Asia     |
|Jack      |Ma       |China    |Asia     |
|Vladimir  |Putin    |Russia   |null     |
|Maria     |Sharapova|Russia   |null     |
+----------+---------+---------+---------+
```

Let's view the transaction log and confirm that only the China partition was updated. Here are the contents of the `_delta_log/00000000000000000001.json` file:

```
{
  "add":{
    "path":"country=China/part-00000-3abbdd5f-1f0f-48bd-8618-5992823d1a37.c000.snappy.parquet",
    "partitionValues":{
      "country":"China"
    },
    "size":854,
    "modificationTime":1571835829000,
    "dataChange":true
  }
}

{
  "remove":{
    "path":"country=China/part-00059-dfa81c0d-2a5e-443c-9e15-1e5c40834d68.c000.snappy.parquet",
    "deletionTimestamp":1571835830680,
    "dataChange":true
  }
}
```

## Practical use case

`replaceWhere` is particularly useful when you have to run a computationally expensive algorithm, but only on certain partitions.

Suppose you have a `personLikesSalsa()` algorithm that is super complex and cannot be run on the entire dataset for performance reasons.

If your dataset is partitioned by country, you can specify to only run the `personLikesSalsa()` algorithm on the most relevant partitions (e.g. Puerto Rico, Colombia, and Cuba).

It might not be ideal to only run an algorithm on a certain partition of your data, but it might be a reality you're forced to face.

## Summary

`replaceWhere` is a powerful option when maintaining Delta data lakes. Performance optimizations like `replaceWhere` are vital when you're working with a big dataset.
