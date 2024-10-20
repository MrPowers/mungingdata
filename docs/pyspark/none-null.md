---
title: "Navigating None and null in PySpark"
date: "2021-06-21"
categories: 
  - "pyspark"
---

This blog post shows you how to gracefully handle `null` in PySpark and how to avoid `null` input errors.

Mismanaging the `null` case is a common source of errors and frustration in PySpark.

Following the tactics outlined in this post will save you from a lot of pain and production bugs.

## Create DataFrames with null values

Let's start by creating a DataFrame with `null` values:

```
df = spark.createDataFrame([(1, None), (2, "li")], ["num", "name"])
df.show()
```

```
+---+----+
|num|name|
+---+----+
|  1|null|
|  2|  li|
+---+----+
```

You use `None` to create DataFrames with `null` values.

`null` is not a value in Python, so this code will not work:

```
df = spark.createDataFrame([(1, null), (2, "li")], ["num", "name"])
```

It throws the following error:

```
NameError: name 'null' is not defined
```

## Read CSVs with null values

Suppose you have the following data stored in the `some_people.csv` file:

```
first_name,age
luisa,23
"",45
bill,
```

Read this file into a DataFrame and then show the contents to demonstrate which values are read into the DataFrame as `null`.

```
path = "/Users/powers/data/some_people.csv"
df = spark.read.option("header", True).csv(path)
df.show()
```

```
+----------+----+
|first_name| age|
+----------+----+
|     luisa|  23|
|      null|  45|
|      bill|null|
+----------+----+
```

The empty string in row 2 and the missing value in row 3 are both read into the PySpark DataFrame as `null` values.

## isNull

Create a DataFrame with `num1` and `num2` columns.

```
df = spark.createDataFrame([(1, None), (2, 2), (None, None)], ["num1", "num2"])
df.show()
```

```
+----+----+
|num1|num2|
+----+----+
|   1|null|
|   2|   2|
|null|null|
+----+----+
```

Append an `is_num2_null` column to the DataFrame:

```
df.withColumn("is_num2_null", df.num2.isNull()).show()
```

```
+----+----+------------+
|num1|num2|is_num2_null|
+----+----+------------+
|   1|null|        true|
|   2|   2|       false|
|null|null|        true|
+----+----+------------+
```

The `isNull` function returns `True` if the value is `null` and `False` otherwise.

## equality

Let's look at how the `==` equality operator handles comparisons with null values.

```
df.withColumn("num1==num2", df.num1 == df.num2).show()
```

```
+----+----+----------+
|num1|num2|num1==num2|
+----+----+----------+
|   1|null|      null|
|   2|   2|      true|
|null|null|      null|
+----+----+----------+
```

If either, or both, of the operands are `null`, then `==` returns `null`.

Lots of times, you'll want this equality behavior:

- When one value is `null` and the other is not `null`, return `False`
- When both values are `null`, return `True`

Here's one way to perform a null safe equality comparison:

```
df.withColumn(
  "num1_eq_num2",
  when(df.num1.isNull() & df.num2.isNull(), True)
    .when(df.num1.isNull() | df.num2.isNull(), False)
    .otherwise(df.num1 == df.num2)
).show()
```

```
+----+----+------------+
|num1|num2|num1_eq_num2|
+----+----+------------+
|   1|null|       false|
|   2|   2|        true|
|null|null|        true|
+----+----+------------+
```

Let's look at a built-in function that lets you perform null safe equality comparisons with less typing.

## null safe equality

We can perform the same null safe equality comparison with the built-in `eqNullSafe` function.

```
df.withColumn("num1_eqNullSafe_num2", df.num1.eqNullSafe(df.num2)).show()
```

```
+----+----+--------------------+
|num1|num2|num1_eqNullSafe_num2|
+----+----+--------------------+
|   1|null|               false|
|   2|   2|                true|
|null|null|                true|
+----+----+--------------------+
```

`eqNullSafe` saves you from extra code complexity. This function is often used when joining DataFrames.

## User Defined Function pitfalls

This section shows a UDF that works on DataFrames without `null` values and fails for DataFrames with `null` values. It then shows how to refactor the UDF so it doesn't error out for `null` values.

Start by creating a DataFrame that does not contain `null` values.

```
countries1 = spark.createDataFrame([("Brasil", 1), ("Mexico", 2)], ["country", "id"])
countries1.show()
```

```
+-------+---+
|country| id|
+-------+---+
| Brasil|  1|
| Mexico|  2|
+-------+---+
```

Create a UDF that appends the string "is fun!".

```
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def bad_funify(s):
     return s + " is fun!"
```

Run the UDF and observe that is works for DataFrames that don't contain any `null` values.

```
countries1.withColumn("fun_country", bad_funify("country")).show()
```

```
+-------+---+--------------+
|country| id|   fun_country|
+-------+---+--------------+
| Brasil|  1|Brasil is fun!|
| Mexico|  2|Mexico is fun!|
+-------+---+--------------+
```

Let's create another DataFrame and run the `bad_funify` function again.

```
countries2 = spark.createDataFrame([("Thailand", 3), (None, 4)], ["country", "id"])
countries2.withColumn("fun_country", bad_funify("country")).show()
```

This code will error out cause the `bad_funify` function can't handle `null` values. Here's the stack trace:

```
org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 604, in main
    process()
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 596, in process
    serializer.dump_stream(out_iter, outfile)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 211, in dump_stream
    self.serializer.dump_stream(self._batched(iterator), stream)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 132, in dump_stream
    for obj in iterator:
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 200, in _batched
    for item in iterator:
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 450, in mapper
    result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 450, in <genexpr>
    result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 85, in <lambda>
    return lambda *a: f(*a)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/util.py", line 73, in wrapper
    return f(*args, **kwargs)
  File "<stdin>", line 3, in bad_funify
TypeError: unsupported operand type(s) for +: 'NoneType' and 'str'

    at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
    at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
    at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
    at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
    at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
    at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
    at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
    at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
    at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
    at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
    at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:345)
    at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:898)
    at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:898)
    at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
    at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:373)
    at org.apache.spark.rdd.RDD.iterator(RDD.scala:337)
    at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
    at org.apache.spark.scheduler.Task.run(Task.scala:131)
    at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
    at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
    at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:748)
21/06/19 19:58:35 WARN TaskSetManager: Lost task 2.0 in stage 45.0 (TID 110) (192.168.1.74 executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 604, in main
    process()
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 596, in process
    serializer.dump_stream(out_iter, outfile)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 211, in dump_stream
    self.serializer.dump_stream(self._batched(iterator), stream)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 132, in dump_stream
    for obj in iterator:
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 200, in _batched
    for item in iterator:
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 450, in mapper
    result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 450, in <genexpr>
    result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 85, in <lambda>
    return lambda *a: f(*a)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/util.py", line 73, in wrapper
    return f(*args, **kwargs)
  File "<stdin>", line 3, in bad_funify
TypeError: unsupported operand type(s) for +: 'NoneType' and 'str'

    at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:517)
    at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:84)
    at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.read(PythonUDFRunner.scala:67)
    at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:470)
    at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
    at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
    at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
    at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source)
    at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
    at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:755)
    at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:345)
    at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:898)
    at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:898)
    at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
    at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:373)
    at org.apache.spark.rdd.RDD.iterator(RDD.scala:337)
    at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
    at org.apache.spark.scheduler.Task.run(Task.scala:131)
    at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:497)
    at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1439)
    at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:500)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:748)

21/06/19 19:58:35 ERROR TaskSetManager: Task 2 in stage 45.0 failed 1 times; aborting job
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/dataframe.py", line 484, in show
    print(self._jdf.showString(n, 20, vertical))
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.PythonException:
  An exception was thrown from the Python worker. Please see the stack trace below.
Traceback (most recent call last):
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 604, in main
    process()
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 596, in process
    serializer.dump_stream(out_iter, outfile)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 211, in dump_stream
    self.serializer.dump_stream(self._batched(iterator), stream)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 132, in dump_stream
    for obj in iterator:
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/serializers.py", line 200, in _batched
    for item in iterator:
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 450, in mapper
    result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 450, in <genexpr>
    result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/worker.py", line 85, in <lambda>
    return lambda *a: f(*a)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/pyspark.zip/pyspark/util.py", line 73, in wrapper
    return f(*args, **kwargs)
  File "<stdin>", line 3, in bad_funify
TypeError: unsupported operand type(s) for +: 'NoneType' and 'str'
```

Let's write a `good_funify` function that won't error out.

```
@udf(returnType=StringType())
def good_funify(s):
     return None if s == None else s + " is fun!"

countries2.withColumn("fun_country", good_funify("country")).show()
```

```
+--------+---+----------------+
| country| id|     fun_country|
+--------+---+----------------+
|Thailand|  3|Thailand is fun!|
|    null|  4|            null|
+--------+---+----------------+
```

Always make sure to handle the null case whenever you write a UDF. It's really annoying to write a function, build a wheel file, and attach it to a cluster, only to have it error out when run on a production dataset that contains `null` values.

## Built-in PySpark functions gracefully handle null

All of the built-in PySpark functions gracefully handle the `null` input case by simply returning `null`. They don't error out. `null` values are common and writing PySpark code would be really tedious if erroring out was the default behavior.

Let's write a `best_funify` function that uses the built-in PySpark functions, so we don't need to explicitly handle the `null` case ourselves.

```
def best_funify(col):
     return concat(col, lit(" is fun!"))

countries2.withColumn("fun_country", best_funify(countries2.country)).show()
```

```
+--------+---+----------------+
| country| id|     fun_country|
+--------+---+----------------+
|Thailand|  3|Thailand is fun!|
|    null|  4|            null|
+--------+---+----------------+
```

It's always best to use built-in PySpark functions whenever possible. They handle the `null` case and save you the hassle.

There are other benefits of built-in PySpark functions, see the article on User Defined Functions for more information.

## nullability

Each column in a DataFrame has a `nullable` property that can be set to `True` or `False`.

If `nullable` is set to `False` then the column cannot contain `null` values.

Here's how to create a DataFrame with one column that's nullable and another column that is not.

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

Use the `printSchema` function to check the `nullable` flag:

```
df.printSchema()

root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)
```

In theory, you can write code that doesn't explicitly handle the `null` case when working with the `age` column because the nullable flag means it doesn't contain null values. In practice, the `nullable` flag is a weak guarantee and you should always write code that handles the null case (or rely on built-in PySpark functions to gracefully handle the null case for you).

See the blog post on DataFrame schemas for more information about controlling the `nullable` property, including unexpected behavior in some cases.

## Testing the null case

You should always make sure your code works properly with null input in the test suite.

Let's look at a helper function from the [quinn](https://github.com/MrPowers/quinn/) library that converts all the whitespace in a string to single spaces.

```
def single_space(col):
    return F.trim(F.regexp_replace(col, " +", " "))
```

Let's look at the test for this function.

```
def test_single_space(spark):
    df = spark.create_df(
        [
            ("  I like     fish  ", "I like fish"),
            ("    zombies", "zombies"),
            ("simpsons   cat lady", "simpsons cat lady"),
            (None, None),
        ],
        [
            ("words", StringType(), True),
            ("expected", StringType(), True),
        ],
    )
    actual_df = df.withColumn("words_single_spaced", quinn.single_space(F.col("words")))
    chispa.assert_column_equality(actual_df, "words_single_spaced", "expected")
```

The `(None, None)` row verifies that the `single_space` function returns `null` when the input is `null`.

The desired function output for `null` input (returning `null` or erroring out) should be documented in the test suite.

## Next steps

You've learned how to effectively manage `null` and prevent it from becoming a pain in your codebase.

`null` values are a common source of errors in PySpark applications, especially when you're writing User Defined Functions.

Get in the habit of verifying that your code gracefully handles `null` input in your test suite to avoid production bugs.
