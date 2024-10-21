---
title: "Delta Lake schema enforcement and evolution with mergeSchema and overwriteSchema"
date: "2019-10-25"
categories: 
  - "delta-lake"
---

# Delta Lake schema enforcement and evolution with mergeSchema and overwriteSchema

Delta lakes prevent data with incompatible schema from being written, unlike Parquet lakes which allow for any data to get written.

Let's demonstrate how Parquet allows for files with incompatible schemas to get written to the same data store. Then let's explore how Delta prevents incompatible data from getting written with schema enforcement.

We'll finish with an explanation of schema evolution.

## Parquet allows for incompatible schemas

Let's create a Parquet with `num1` and `num2` columns:

We'll use the [spark-daria](https://github.com/MrPowers/spark-daria/) createDF method to build DataFrames for these examples.

```
val df = spark.createDF(
  List(
    (1, 2),
    (3, 4)
  ), List(
    ("num1", IntegerType, true),
    ("num2", IntegerType, true)
  )
)

val parquetPath = new java.io.File("./tmp/parquet_schema/").getCanonicalPath

df.write.parquet(parquetPath)
```

Let's view the contents of the Parquet lake.

```
spark.read.parquet(parquetPath).show()

+----+----+
|num1|num2|
+----+----+
|   1|   2|
|   3|   4|
+----+----+
```

Let's create another Parquet file with only a `num2` column and append it to the same folder.

```
val df2 = spark.createDF(
  List(
    88,
    99
  ), List(
    ("num2", IntegerType, true)
  )
)

df2.write.mode("append").parquet(parquetPath)
```

Let's read the Parquet lake into a DataFrame and view the output that's undesirable.

```
spark.read.parquet(parquetPath).show()

+----+
|num2|
+----+
|   2|
|   4|
|  88|
|  99|
+----+
```

We lost the `num1` column! `spark.read.parquet` is only returning a DataFrame with the `num2` column.

This isn't ideal. Let's see if Delta provides a better result.

## Delta automatic schema updates

Let's create the same `df` as earlier and write out a Delta data lake.

```
val df = spark.createDF(
  List(
    (1, 2),
    (3, 4)
  ), List(
    ("num1", IntegerType, true),
    ("num2", IntegerType, true)
  )
)

val deltaPath = new java.io.File("./tmp/schema_example/").getCanonicalPath

df.write.format("delta").save(deltaPath)
```

The Delta table starts with two columns, as expected:

```
spark.read.format("delta").load(deltaPath).show()

+----+----+
|num1|num2|
+----+----+
|   1|   2|
|   3|   4|
+----+----+
```

Let's append a file with only the `num1` column to the Delta lake and see how Delta handles the schema mismatch.

```
val df2 = spark.createDF(
  List(
    88,
    99
  ), List(
    ("num1", IntegerType, true)
  )
)

df2.write.format("delta").mode("append").save(deltaPath)
```

Delta gracefully fills in missing column values with `nulls`.

```
spark.read.format("delta").load(deltaPath).show()

+----+----+
|num1|num2|
+----+----+
|   1|   2|
|   3|   4|
|  88|null|
|  99|null|
+----+----+
```

Let's append a DataFrame that only has a `num2` column to make sure Delta also handles that case gracefully.

```
val df3 = spark.createDF(
  List(
    101,
    102
  ), List(
    ("num2", IntegerType, true)
  )
)

df3.write.format("delta").mode("append").save(deltaPath)
```

We can see Delta gracefully populates the `num2` values and nulls out the `num1` values.

```
spark.read.format("delta").load(deltaPath).show()

+----+----+
|num1|num2|
+----+----+
|   1|   2|
|   3|   4|
|  88|null|
|  99|null|
|null| 101|
|null| 102|
+----+----+
```

Let's see if Delta can handle a DataFrame with `num1`, `num2`, and `num3` columns.

```
val df4 = spark.createDF(
  List(
    (7, 7, 7),
    (8, 8, 8)
  ), List(
    ("num1", IntegerType, true),
    ("num2", IntegerType, true),
    ("num3", IntegerType, true)
  )
)

df4.write.format("delta").mode("append").save(deltaPath)
```

This causes the code to error out with the following message:

```
org.apache.spark.sql.AnalysisException: A schema mismatch detected when writing to the Delta table.
To enable schema migration, please set:
'.option("mergeSchema", "true")'.

Table schema:
root
-- num1: integer (nullable = true)
-- num2: integer (nullable = true)


Data schema:
root
-- num1: integer (nullable = true)
-- num2: integer (nullable = true)
-- num3: integer (nullable = true)
```

## mergeSchema

We can fix this by setting `mergeSchema` to `true`, as indicated by the error message.

The codes works perfectly once the option is set:

```
df4
  .write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "true")
  .save(deltaPath)

spark.read.format("delta").load(deltaPath).show()

+----+----+----+
|num1|num2|num3|
+----+----+----+
|   7|   7|   7|
|   8|   8|   8|
|   1|   2|null|
|   3|   4|null|
|null| 101|null|
|null| 102|null|
|  88|null|null|
|  99|null|null|
+----+----+----+
```

## Replace table schema

`mergeSchema` will work when you append a file with a completely different schema, but it probably won't give you the result you're looking for.

```
val df5 = spark.createDF(
  List(
    ("nice", "person"),
    ("like", "madrid")
  ), List(
    ("word1", StringType, true),
    ("word2", StringType, true)
  )
)

df5
  .write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "true")
  .save(deltaPath)
```

`mergeSchema` appends two new columns to the DataFrame because the save mode was set to append.

```
spark.read.format("delta").load(deltaPath).show()

+----+----+----+-----+------+
|num1|num2|num3|word1| word2|
+----+----+----+-----+------+
|   7|   7|   7| null|  null|
|   8|   8|   8| null|  null|
|   1|   2|null| null|  null|
|   3|   4|null| null|  null|
|null|null|null| nice|person|
|null|null|null| like|madrid|
|  88|null|null| null|  null|
|  99|null|null| null|  null|
|null| 101|null| null|  null|
|null| 102|null| null|  null|
+----+----+----+-----+------+
```

Let's see how `mergeSchema` behaves when using a completely different schema and setting the save mode to overwrite.

```
df5
  .write
  .format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .save(deltaPath)

spark.read.format("delta").load(deltaPath).show()

+----+----+----+-----+------+
|num1|num2|num3|word1| word2|
+----+----+----+-----+------+
|null|null|null| nice|person|
|null|null|null| like|madrid|
+----+----+----+-----+------+
```

`mergeSchema` isn't the best when the schemas are completely different. It's better for incremental schema changes.

## overwriteSchema

Setting `overwriteSchema` to true will wipe out the old schema and let you create a completely new table.

```
df5
  .write
  .format("delta")
  .option("overwriteSchema", "true")
  .mode("overwrite")
  .save(deltaPath)
```

```
spark.read.format("delta").load(deltaPath).show()

+-----+------+
|word1| word2|
+-----+------+
| nice|person|
| like|madrid|
+-----+------+
```

## Conclusion

Delta lakes offer powerful schema evolution features that are not available in Parquet lakes.

Delta lakes also enforce schemas and make it less likely that a bad write will mess up your entire lake.

Delta offers some great features that are simply not available in plain vanilla Parquet lakes.
