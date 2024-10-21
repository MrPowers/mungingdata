---
title: "Defining PySpark Schemas with StructType and StructField"
date: "2021-06-26"
categories: 
  - "pyspark"
---

# Defining PySpark Schemas with StructType and StructField

This post explains how to define PySpark schemas and when this design pattern is useful.

It'll also explain when defining schemas seems wise, but can actually be safely avoided.

Schemas are often defined when validating DataFrames, reading in data from CSV files, or when manually constructing DataFrames in your test suite. You'll use all of the information covered in this post frequently when writing PySpark code.

## Access DataFrame schema

Let's create a PySpark DataFrame and then access the schema.

```
df = spark.createDataFrame([(1, "a"), (2, "b")], ["num", "letter"])
df.show()
```

```
+---+------+
|num|letter|
+---+------+
|  1|     a|
|  2|     b|
+---+------+
```

Use the `printSchema()` method to print a human readable version of the schema.

```
df.printSchema()

root
 |-- num: long (nullable = true)
 |-- letter: string (nullable = true)
```

The `num` column is long type and the `letter` column is string type. We created this DataFrame with the `createDataFrame` method and did not explicitly specify the types of each column. Spark infers the types based on the row values when you don't explicitly provides types.

Use the `schema` attribute to fetch the actual schema object associated with a DataFrame.

```
df.schema

StructType(List(StructField(num,LongType,true),StructField(letter,StringType,true)))
```

The entire schema is stored in a `StructType`. The details for each column in the schema is stored in `StructField` objects. Each `StructField` contains the column name, type, and nullable property.

## Define basic schema

Let's create another DataFrame, but specify the schema ourselves rather than relying on schema inference.

```
from pyspark.sql import Row
from pyspark.sql.types import *

rdd = spark.sparkContext.parallelize([
    Row(name='Allie', age=2),
    Row(name='Sara', age=33),
    Row(name='Grace', age=31)])

schema = schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), False)])

df = spark.createDataFrame(rdd, schema)

df.show()
```

```
+-----+---+
| name|age|
+-----+---+
|Allie|  2|
| Sara| 33|
|Grace| 31|
+-----+---+
```

This example uses the same `createDataFrame` method as earlier, but invokes it with a RDD and a `StructType` (a full schema object).

Use the `printSchema()` method to verify that the DataFrame has the exact schema we specified.

```
df.printSchema()

root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)
```

We can see that the column names, types, and nullable properties are exactly what we specified. Production grade code and test suites often require this fine grained precision.

This post on [creating PySpark DataFrames](https://neapowers.com/pyspark/createdataframe-todf/) discusses another tactic for precisely creating schemas without so much typing.

## Define schema with ArrayType

PySpark DataFrames support array columns. An array can hold different objects, the type of which much be specified when defining the schema.

Let's create a DataFrame with a column that holds an array of integers.

```
rdd = spark.sparkContext.parallelize([
    Row(letter="a", nums=[1, 2, 3]),
    Row(letter="b", nums=[4, 5, 6])])

schema = schema = StructType([
    StructField("letter", StringType(), True),
    StructField("nums", ArrayType(IntegerType(), False))
])

df = spark.createDataFrame(rdd, schema)

df.show()
```

```
+------+---------+
|letter|     nums|
+------+---------+
|     a|[1, 2, 3]|
|     b|[4, 5, 6]|
+------+---------+
```

Print the schema to view the ArrayType column.

```
df.printSchema()

root
 |-- letter: string (nullable = true)
 |-- nums: array (nullable = true)
 |    |-- element: integer (containsNull = false)
```

Array columns are useful for a variety of PySpark analyses.

## Nested schemas

Schemas can also be nested. Let's build a DataFrame with a `StructType` within a `StructType`.

```
rdd = spark.sparkContext.parallelize([
    Row(first_name="reggie", stats=Row(home_runs=563, batting_average=0.262)),
    Row(first_name="barry", stats=Row(home_runs=762, batting_average=0.298))])

schema = schema = StructType([
    StructField("letter", StringType(), True),
    StructField(
        "stats",
        StructType([
            StructField("home_runs", IntegerType(), False),
            StructField("batting_average", DoubleType(), False),
        ]),
        True
    ),
])

df = spark.createDataFrame(rdd, schema)

df.show()
```

```
+------+------------+
|letter|       stats|
+------+------------+
|reggie|{563, 0.262}|
| barry|{762, 0.298}|
+------+------------+
```

Let's print the nested schema:

```
df.printSchema()

root
 |-- letter: string (nullable = true)
 |-- stats: struct (nullable = true)
 |    |-- home_runs: integer (nullable = false)
 |    |-- batting_average: double (nullable = false)
```

Nested schemas allow for a powerful way to organize data, but they also introduction additional complexities. It's generally easier to work with flat schemas, but nested (and deeply nested schemas) also allow for elegant solutions to certain problems.

## Reading CSV files

When reading a CSV file, you can either rely on schema inference or specify the schema yourself.

For data exploration, schema inference is usually fine. You don't have to be overly concerned about types and nullable properties when you're just getting to know a dataset.

For production applications, it's best to explicitly define the schema and avoid inference. You don't want to rely on fragile inference rules that may get updated and cause unanticipated changes in your code.

Parquet files contain the schema information in the file footer, so you get the best of both worlds. You don't have to rely on schema inference and don't have to tediously define the schema yourself. This is one of many reasons why Parquet files are almost always better than CSV files in data analyses.

## Validations

Suppose you're working with a data vendor that gives you an updated CSV file on a weekly basis that you need to ingest into your systems.

The first step of your ingestion pipeline should be to validate that the schema of the file is what you expect. You don't want to ingest a file, and potentially corrupt a data lake, because the data vendor made some changes to the input file.

The [quinn](https://github.com/MrPowers/quinn) data validation helper methods can assist you in validating schemas. You'll of course need to specify the expected schema, using the tactics outlined in this post, to invoke the schema validation checks.

## Test suites

PySpark code is often tested by comparing two DataFrames or comparing two columns within a DataFrame. Creating DataFrames requires building schemas, using the tactics outlined in this post. See this post for more information on [Testing PySpark Applications](https://mungingdata.com/pyspark/testing-pytest-chispa/).

## Next steps

PySpark exposes elegant schema specification APIs that help you create DataFrames, build reliable tests, and construct robust data pipelines.

You'll be building PySpark schemas frequently so you might as well just memorize the syntax.
