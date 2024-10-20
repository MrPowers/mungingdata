---
title: "Converting a PySpark Map / Dictionary to Multiple Columns"
date: "2020-07-22"
categories: 
  - "pyspark"
---

Python dictionaries are stored in PySpark map columns (the `pyspark.sql.types.MapType` class). This blog post explains how to convert a map into multiple columns.

You'll want to break up a map to multiple columns for performance gains and when writing data to different types of data stores. It's typically best to avoid writing complex columns.

## Creating a DataFrame with a MapType column

Let's create a DataFrame with a map column called `some_data`:

```
data = [("jose", {"a": "aaa", "b": "bbb"}), ("li", {"b": "some_letter", "z": "zed"})]
df = spark.createDataFrame(data, ["first_name", "some_data"])
df.show(truncate=False)
```

```
+----------+----------------------------+
|first_name|some_data                   |
+----------+----------------------------+
|jose      |[a -> aaa, b -> bbb]        |
|li        |[b -> some_letter, z -> zed]|
+----------+----------------------------+
```

Use `df.printSchema` to verify the type of the `some_data` column:

```
root
 |-- first_name: string (nullable = true)
 |-- some_data: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
```

You can see `some_data` is a MapType column with string keys and values.

Add a `some_data_a` column that grabs the value associated with the key `a` in the `some_data` column. The `getItem` method helps when fetching values from PySpark maps.

```
df.withColumn("some_data_a", F.col("some_data").getItem("a")).show(truncate=False)
```

```
+----------+----------------------------+-----------+
|first_name|some_data                   |some_data_a|
+----------+----------------------------+-----------+
|jose      |[a -> aaa, b -> bbb]        |aaa        |
|li        |[b -> some_letter, z -> zed]|null       |
+----------+----------------------------+-----------+
```

This syntax also works:

```
df.withColumn("some_data_a", F.col("some_data")["a"]).show()
```

## Manually expanding the DataFrame

We can manually append the `some_data_a`, `some_data_b`, and `some_data_z` columns to our DataFrame as follows:

```
df\
    .withColumn("some_data_a", F.col("some_data").getItem("a"))\
    .withColumn("some_data_b", F.col("some_data").getItem("b"))\
    .withColumn("some_data_z", F.col("some_data").getItem("z"))\
    .show(truncate=False)
```

```
+----------+----------------------------+-----------+-----------+-----------+
|first_name|some_data                   |some_data_a|some_data_b|some_data_z|
+----------+----------------------------+-----------+-----------+-----------+
|jose      |[a -> aaa, b -> bbb]        |aaa        |bbb        |null       |
|li        |[b -> some_letter, z -> zed]|null       |some_letter|zed        |
+----------+----------------------------+-----------+-----------+-----------+
```

We can refactor this code to be more concise and to generate a more efficient parsed logical plan.

```
cols = [F.col("first_name")] + list(map(
    lambda f: F.col("some_data").getItem(f).alias(str(f)),
    ["a", "b", "z"]))
df.select(cols).show()
```

```
+----------+----+-----------+----+
|first_name|   a|          b|   z|
+----------+----+-----------+----+
|      jose| aaa|        bbb|null|
|        li|null|some_letter| zed|
+----------+----+-----------+----+
```

Manually appending the columns is fine if you know all the distinct keys in the map. If you don't know all the distinct keys, you'll need a programatic solution, but be warned - this approach is slow!

## Programatically expanding the DataFrame

Here's the code to programatically expand the DataFrame (keep reading to see all the steps broken down individually):

```
keys_df = df.select(F.explode(F.map_keys(F.col("some_data")))).distinct()
keys = list(map(lambda row: row[0], keys_df.collect()))
key_cols = list(map(lambda f: F.col("some_data").getItem(f).alias(str(f)), keys))
final_cols = [F.col("first_name")] + key_cols
df.select(final_cols).show()
```

```
+----------+----+-----------+----+
|first_name|   z|          b|   a|
+----------+----+-----------+----+
|      jose|null|        bbb| aaa|
|        li| zed|some_letter|null|
+----------+----+-----------+----+
```

Let's break down each step of this code.

**Step 1**: Create a DataFrame with all the unique keys

```
keys_df = df.select(F.explode(F.map_keys(F.col("some_data")))).distinct()
keys_df.show()
```

```
+---+
|col|
+---+
|  z|
|  b|
|  a|
+---+
```

**Step 2**: Convert the DataFrame to a list with all the unique keys

```
keys = list(map(lambda row: row[0], keys_df.collect()))
print(keys) # => ['z', 'b', 'a']
```

The `collect()` method gathers all the data on the driver node, which can be slow. We call `distinct()` to limit the data that's being collected on the driver node. Spark is a big data engine that's optimized for running computations in parallel on multiple nodes in a cluster. Collecting data on a single node and leaving the worker nodes idle should be avoided whenever possible. We're only using `collect()` here cause it's the only option.

**Step 3**: Create an array of column objects for the map items

```
key_cols = list(map(lambda f: F.col("some_data").getItem(f).alias(str(f)), keys))
print(key_cols)
# => [Column<b'some_data[z] AS `z`'>, Column<b'some_data[b] AS `b`'>, Column<b'some_data[a] AS `a`'>]
```

**Step 4**: Add any additional columns before calculating the final result

```
final_cols = [F.col("first_name")] + key_cols
print(final_cols)
# => [Column<b'first_name'>, Column<b'some_data[z] AS `z`'>, Column<b'some_data[b] AS `b`'>, Column<b'some_data[a] AS `a`'>]
```

**Step 5**: Run a `select()` to get the final result

```
df.select(final_cols).show()
```

```
+----------+----+-----------+----+
|first_name|   z|          b|   a|
+----------+----+-----------+----+
|      jose|null|        bbb| aaa|
|        li| zed|some_letter|null|
+----------+----+-----------+----+
```

Step 2 is the potential bottleneck. If there aren't too many unique keys it shouldn't be too slow.

Steps 3 and 4 should run very quickly. Running a single select operation in Step 5 is also quick.

## Examining logical plans

Use the `explain()` function to print the logical plans and see if the parsed logical plan needs a lot of optimizations:

```
df.select(final_cols).explain(True)

== Parsed Logical Plan ==
'Project [unresolvedalias('first_name, None), 'some_data[z] AS z#28, 'some_data[b] AS b#29, 'some_data[a] AS a#30]
+- LogicalRDD [first_name#0, some_data#1], false

== Analyzed Logical Plan ==
first_name: string, z: string, b: string, a: string
Project [first_name#0, some_data#1[z] AS z#28, some_data#1[b] AS b#29, some_data#1[a] AS a#30]
+- LogicalRDD [first_name#0, some_data#1], false

== Optimized Logical Plan ==
Project [first_name#0, some_data#1[z] AS z#28, some_data#1[b] AS b#29, some_data#1[a] AS a#30]
+- LogicalRDD [first_name#0, some_data#1], false

== Physical Plan ==
*(1) Project [first_name#0, some_data#1[z] AS z#28, some_data#1[b] AS b#29, some_data#1[a] AS a#30]
```

As you can see the parsed logical plan is quite similar to the optimized logical plan. Catalyst does not need to perform a lot of optimizations, so our code is efficient.

## Next steps

Breaking out a MapType column into multiple columns is fast if you know all the distinct map key values, but potentially slow if you need to figure them all out dynamically.

You would want to avoid calculating the unique map keys whenever possible. Consider storing the distinct values in a data store and updating it incrementally if you have production workflows that depend on the distinct keys.

If breaking out your map into separate columns is slow, consider segmenting your job into two steps:

- Step 1: Break the map column into separate columns and write it out to disk
- Step 2: Read the new dataset with separate columns and perform the rest of your analysis

Complex column types are important for a lot of Spark analyses. In general favor StructType columns over MapType columns because they're easier to work with.
