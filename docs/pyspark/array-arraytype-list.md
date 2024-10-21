---
title: "Working with PySpark ArrayType Columns"
date: "2021-06-28"
categories: 
  - "pyspark"
---

# Working with PySpark ArrayType Columns

This post explains how to create DataFrames with ArrayType columns and how to perform common data processing operations.

Array columns are one of the most useful column types, but they're hard for most Python programmers to grok. The PySpark array syntax isn't similar to the list comprehension syntax that's normally used in Python.

This post covers the important PySpark array operations and highlights the pitfalls you should watch out for.

## Create ArrayType column

Create a DataFrame with an array column.

```
df = spark.createDataFrame(
    [("abc", [1, 2]), ("cd", [3, 4])], ["id", "numbers"]
)
df.show()
```

```
+---+-------+
| id|numbers|
+---+-------+
|abc| [1, 2]|
| cd| [3, 4]|
+---+-------+
```

Print the schema of the DataFrame to verify that the `numbers` column is an array.

```
df.printSchema()

root
 |-- id: string (nullable = true)
 |-- numbers: array (nullable = true)
 |    |-- element: long (containsNull = true)
```

`numbers` is an array of long elements.

We can also create this DataFrame using the explicit `StructType` syntax.

```
from pyspark.sql.types import *
from pyspark.sql import Row

rdd = spark.sparkContext.parallelize(
    [Row("abc", [1, 2]), Row("cd", [3, 4])]
)

schema = StructType([
    StructField("id", StringType(), True),
    StructField("numbers", ArrayType(IntegerType(), True), True)
])

df = spark.createDataFrame(rdd, schema)

df.show()
```

```
+---+-------+
| id|numbers|
+---+-------+
|abc| [1, 2]|
| cd| [3, 4]|
+---+-------+
```

The explicit syntax makes it clear that we're creating an `ArrayType` column.

## Fetch value from array

Add a `first_number` column to the DataFrame that returns the first element in the `numbers` array.

```
df.withColumn("first_number", df.numbers[0]).show()
```

```
+---+-------+------------+
| id|numbers|first_number|
+---+-------+------------+
|abc| [1, 2]|           1|
| cd| [3, 4]|           3|
+---+-------+------------+
```

The PySpark array indexing syntax is similar to list indexing in vanilla Python.

## Combine columns to array

The `array` method makes it easy to combine multiple DataFrame columns to an array.

Create a DataFrame with `num1` and `num2` columns:

```
df = spark.createDataFrame(
    [(33, 44), (55, 66)], ["num1", "num2"]
)
df.show()
```

```
+----+----+
|num1|num2|
+----+----+
|  33|  44|
|  55|  66|
+----+----+
```

Add a `nums` column, which is an array that contains `num1` and `num2`:

```
from pyspark.sql.functions import *

df.withColumn("nums", array(df.num1, df.num2)).show()
```

```
+----+----+--------+
|num1|num2|    nums|
+----+----+--------+
|  33|  44|[33, 44]|
|  55|  66|[55, 66]|
+----+----+--------+
```

## List aggregations

Collecting values into a list can be useful when performing aggregations. This section shows how to create an `ArrayType` column with a group by aggregation that uses `collect_list`.

Create a DataFrame with `first_name` and `color` columns that indicate colors some individuals like.

```
df = spark.createDataFrame(
    [("joe", "red"), ("joe", "blue"), ("lisa", "yellow")], ["first_name", "color"]
)

df.show()
```

```
+----------+------+
|first_name| color|
+----------+------+
|       joe|   red|
|       joe|  blue|
|      lisa|yellow|
+----------+------+
```

Group by `first_name` and create an `ArrayType` column with all the colors a given `first_name` likes.

```
res = (df
    .groupBy(df.first_name)
    .agg(collect_list(col("color")).alias("colors")))

res.show()
```

```
+----------+-----------+
|first_name|     colors|
+----------+-----------+
|      lisa|   [yellow]|
|       joe|[red, blue]|
+----------+-----------+
```

Print the schema to verify that `colors` is an `ArrayType` column.

```
res.printSchema()

root
 |-- first_name: string (nullable = true)
 |-- colors: array (nullable = false)
 |    |-- element: string (containsNull = false)
```

`collect_list` shows that some of Spark's API methods take advantage of `ArrayType` columns as well.

## Exploding an array into multiple rows

A PySpark array can be exploded into multiple rows, the opposite of `collect_list`.

Create a DataFrame with an `ArrayType` column:

```
df = spark.createDataFrame(
    [("abc", [1, 2]), ("cd", [3, 4])], ["id", "numbers"]
)

df.show()
```

```
+---+-------+
| id|numbers|
+---+-------+
|abc| [1, 2]|
| cd| [3, 4]|
+---+-------+
```

Explode the array column, so there is only one number per DataFrame row.

```
df.select(col("id"), explode(col("numbers")).alias("number")).show()
```

```
+---+------+
| id|number|
+---+------+
|abc|     1|
|abc|     2|
| cd|     3|
| cd|     4|
+---+------+
```

`collect_list` collapses multiple rows into a single row. `explode` does the opposite and expands an array into multiple rows.

## Advanced operations

You can manipulate PySpark arrays similar to how regular Python lists are processed with `map()`, `filter()`, and `reduce()`.

Complete discussions for these advance operations are broken out in separate posts:

- [filtering PySpark arrays](https://mungingdata.com/pyspark/filter-array/)
- mapping PySpark arrays with transform
- reducing PySpark arrays with aggregate
- [merging PySpark arrays](https://mungingdata.com/pyspark/concat-array-union-except-intersect/)
- [exists and forall](https://mungingdata.com/pyspark/exists-forall-any-all-array/)

These methods make it easier to perform advance PySpark array operations. In earlier versions of PySpark, you needed to use user defined functions, which are slow and hard to work with.

A PySpark DataFrame column can also be converted to a regular Python list, [as described in this post](https://mungingdata.com/pyspark/column-to-list-collect-tolocaliterator/). This only works for small DataFrames, see the linked post for the detailed discussion.

## Writing to files

You can write DataFrames with array columns to Parquet files without issue.

```
df = spark.createDataFrame(
    [("abc", [1, 2]), ("cd", [3, 4])], ["id", "numbers"]
)

parquet_path = "/Users/powers/Documents/tmp/parquet_path"
df.write.parquet(parquet_path)
```

You cannot write DataFrames with array columns to CSV files:

```
csv_path = "/Users/powers/Documents/tmp/csv_path"
df.write.csv(csv_path)
```

Here's the error you'll get:

```
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/readwriter.py", line 1372, in csv
    self._jwrite.csv(path)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: CSV data source does not support array<bigint> data type.
```

This isn't a limitation of Spark - it's a limitation of the CSV file format. CSV files can't handle complex column types like arrays. Parquet files are able to handle complex columns.

## Unanticipated type conversions

Let's create a DataFrame with an integer column and a string column to demonstrate the surprising type conversion that takes place when different types are combined in a PySpark array.

```
df = spark.createDataFrame(
    [("a", 8), ("b", 9)], ["letter", "number"]
)
df.show()
```

```
+------+------+
|letter|number|
+------+------+
|     a|     8|
|     b|     9|
+------+------+
```

Combine the `letter` and `number` columns into an array and then fetch the number from the array.

```
res = (df
  .withColumn("arr", array(df.letter, df.number))
  .withColumn("number2", col("arr")[1]))

res.show()
```

```
+------+------+------+-------+
|letter|number|   arr|number2|
+------+------+------+-------+
|     a|     8|[a, 8]|      8|
|     b|     9|[b, 9]|      9|
+------+------+------+-------+
```

Print the schema to observe the `number2` column is string type.

```
res.printSchema()

root
 |-- letter: string (nullable = true)
 |-- number: long (nullable = true)
 |-- arr: array (nullable = false)
 |    |-- element: string (containsNull = true)
 |-- number2: string (nullable = true)
```

Regular Python lists can hold values with different types. `my_arr = [1, "a"]` is valid in Python.

PySpark arrays can only hold one type. In order to combine `letter` and `number` in an array, PySpark needs to convert `number` to a string.

PySpark's type conversion causes you to lose valuable type information. It's arguable that the `array` function should error out when joining columns with different types, rather than implicitly converting types.

It's best for you to explicitly convert types when combining different types into a PySpark array rather than relying on implicit conversions.

## Next steps

PySpark arrays are useful in a variety of situations and you should master all the information covered in this post.

Always use the built-in functions when manipulating PySpark arrays and avoid UDFs whenever possible.

PySpark isn't the best for truly massive arrays. As the `explode` and `collect_list` examples show, data can be modelled in multiple rows or in an array. You'll need to tailor your data model based on the size of your data and what's most performant with Spark.

Grok the advanced array operations linked in this article. The native PySpark array API is powerful enough to handle almost all use cases without requiring UDFs.
