---
title: "Dealing with null in Spark"
date: "2019-01-09"
categories: 
  - "apache-spark"
---

# Dealing with null in Spark

Spark Datasets / DataFrames are filled with null values and you should write code that gracefully handles these null values.

You don't want to write code that thows `NullPointerExceptions` - yuck!

If you're using PySpark, see this post on [Navigating None and null in PySpark](https://mungingdata.com/pyspark/none-null/).

[Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) outlines all of the advanced tactics for making null your best friend when you work with Spark.

This post outlines when null should be used, how native Spark functions handle null input, and how to simplify null logic by avoiding user defined functions. This post is a great start, but it doesn't provide all the detailed context discussed in [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/).

<iframe width="560" height="315" src="https://www.youtube.com/embed/zWhH44Tjjco" allowfullscreen></iframe>

## What is null?

In SQL databases, "[null means that some value is unknown, missing, or irrelevant](https://www.itprotoday.com/sql-server/sql-design-reason-null)." The SQL concept of null is different than null in programming languages like JavaScript or Scala. Spark DataFrame best practices are aligned with SQL best practices, so DataFrames should use null for values that are unknown, missing or irrelevant.

## Spark uses null by default sometimes

Let's look at the following file as an example of how Spark considers blank and empty CSV fields as `null` values.

```
name,country,zip_code
joe,usa,89013
ravi,india,
"",,12389
```

All the blank values and empty strings are read into a DataFrame as null by the Spark CSV library ([after Spark 2.0.1 at least](https://medium.com/@mrpowers/sparks-treatment-of-empty-strings-and-null-values-in-csv-files-80748893451f)).

```
val peopleDf = spark.read.option("header", "true").csv(path)
```

```
peopleDf.show()

+----+-------+--------+
|name|country|zip_code|
+----+-------+--------+
| joe|    usa|   89013|
|ravi|  india|    null|
|null|   null|   12389|
+----+-------+--------+
```

The Spark `csv()` method demonstrates that null is used for values that are unknown or missing when files are read into DataFrames.

## nullable Columns

Let's create a DataFrame with a `name` column that isn't nullable and an `age` column that is nullable. The `name` column cannot take null values, but the `age` column can take null values. The nullable property is the third argument when instantiating a `StructField`.

```
val schema = List(
  StructField("name", StringType, false),
  StructField("age", IntegerType, true)
)

val data = Seq(
  Row("miguel", null),
  Row("luisa", 21)
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  StructType(schema)
)
```

If we try to create a DataFrame with a `null` value in the `name` column, the code will blow up with this error: "Error while encoding: java.lang.RuntimeException: The 0th field 'name' of input row cannot be null".

Here's some code that would cause the error to be thrown:

```
val data = Seq(
  Row("phil", 44),
  Row(null, 21)
)
```

You can keep null values out of certain columns by setting `nullable` to `false`.

You won't be able to set `nullable` to `false` for all columns in a DataFrame and pretend like `null` values don't exist. For example, when joining DataFrames, the join column will return `null` when a match cannot be made.

You can run, but you can't hide!

## Native Spark code

Native Spark code handles `null` gracefully.

Let's create a DataFrame with numbers so we have some data to play with.

```
val schema = List(
  StructField("number", IntegerType, true)
)

val data = Seq(
  Row(1),
  Row(8),
  Row(12),
  Row(null)
)

val numbersDF = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  StructType(schema)
)
```

Now let's add a column that returns `true` if the number is even, `false` if the number is odd, and `null` otherwise.

```
numbersDF
  .withColumn("is_even", $"number" % 2 === 0)
  .show()
```

```
+------+-------+
|number|is_even|
+------+-------+
|     1|  false|
|     8|   true|
|    12|   true|
|  null|   null|
+------+-------+
```

The Spark `%` function returns `null` when the input is `null`. Actually all Spark functions return `null` when the input is `null`. All of your Spark functions should return `null` when the input is `null` too!

## Scala null Conventions

Native Spark code cannot always be used and sometimes you'll need to fall back on Scala code and User Defined Functions. The Scala best practices for null are different than the Spark null best practices.

David Pollak, the author of Beginning Scala, stated "Ban null from any of your code. Period." Alvin Alexander, a prominent Scala blogger and author, explains why Option is better than null in [this blog post](https://alvinalexander.com/scala/using-scala-option-some-none-idiom-function-java-null). The Scala community clearly prefers Option to avoid the pesky null pointer exceptions that have burned them in Java.

Some developers erroneously interpret these Scala best practices to infer that null should be banned from DataFrames as well! Remember that DataFrames are akin to SQL databases and should generally follow SQL best practices. Scala best practices are completely different.

The [Databricks Scala style guide](https://github.com/databricks/scala-style-guide#perf-option) does not agree that null should always be banned from Scala code and says: "For performance sensitive code, prefer null over Option, in order to avoid virtual method calls and boxing."

The Spark source code uses the Option keyword 821 times, but it also refers to null directly in code like `if (ids != null)`. Spark may be taking a hybrid approach of using Option when possible and falling back to null when necessary for performance reasons.

I think Option should be used wherever possible and you should only fall back on null when necessary for performance reasons.

Let's dig into some code and see how null and Option can be used in Spark user defined functions.

## User Defined Functions

Let's create a user defined function that returns true if a number is even and false if a number is odd.

```
def isEvenSimple(n: Integer): Boolean = {
  n % 2 == 0
}

val isEvenSimpleUdf = udf[Boolean, Integer](isEvenSimple)
```

Suppose we have the following `sourceDf` DataFrame:

```
+------+
|number|
+------+
|     1|
|     8|
|    12|
|  null|
+------+
```

Our UDF does not handle `null` input values. Let's run the code and observe the error.

```
numbersDF.withColumn(
  "is_even",
  isEvenSimpleUdf(col("number"))
)
```

Here is the error message:

SparkException: Job aborted due to stage failure: Task 2 in stage 16.0 failed 1 times, most recent failure: Lost task 2.0 in stage 16.0 (TID 41, localhost, executor driver): org.apache.spark.SparkException: Failed to execute user defined function($anonfun$1: (int) => boolean)

Caused by: java.lang.NullPointerException

We can use the `isNotNull` method to work around the `NullPointerException` that's caused when `isEvenSimpleUdf` is invoked.

```
val actualDf = sourceDf.withColumn(
  "is_even",
  when(
    col("number").isNotNull,
    isEvenSimpleUdf(col("number"))
  ).otherwise(lit(null))
)
```

```
actualDf.show()

+------+-------+
|number|is_even|
+------+-------+
|     1|  false|
|     8|   true|
|    12|   true|
|  null|   null|
+------+-------+
```

It's better to write user defined functions that gracefully deal with null values and don't rely on the `isNotNull` work around - let's try again.

### Dealing with null badly

Let's refactor the user defined function so it doesn't error out when it encounters a null value.

```
def isEvenBad(n: Integer): Boolean = {
  if (n == null) {
    false
  } else {
    n % 2 == 0
  }
}

val isEvenBadUdf = udf[Boolean, Integer](isEvenBad)
```

We can run the `isEvenBadUdf` on the same `sourceDf` as earlier.

```
val actualDf = sourceDf.withColumn(
  "is_even",
  isEvenBadUdf(col("number"))
)
```

```
actualDf.show()

+------+-------+
|number|is_even|
+------+-------+
|     1|  false|
|     8|   true|
|    12|   true|
|  null|  false|
+------+-------+
```

This code works, but is terrible because it returns false for odd numbers and null numbers. Remember that null should be used for values that are irrelevant. null is not even or odd - returning false for null numbers implies that null is odd!

Let's refactor this code and correctly return null when number is null.

### Dealing with null better

The `isEvenBetterUdf` returns `true` / `false` for numeric values and `null` otherwise.

```
def isEvenBetter(n: Integer): Option[Boolean] = {
  if (n == null) {
    None
  } else {
    Some(n % 2 == 0)
  }
}

val isEvenBetterUdf = udf[Option[Boolean], Integer](isEvenBetter)
```

The `isEvenBetter` method returns an `Option[Boolean]`. When the input is null, `isEvenBetter` returns `None`, which is converted to null in DataFrames.

Let's run the `isEvenBetterUdf` on the same `sourceDf` as earlier and verify that null values are correctly added when the number column is null.

```
val actualDf = sourceDf.withColumn(
  "is_even",
  isEvenBetterUdf(col("number"))
)
```

```
actualDf.show()

+------+-------+
|number|is_even|
+------+-------+
|     1|  false|
|     8|   true|
|    12|   true|
|  null|   null|
+------+-------+
```

The `isEvenBetter` function is still directly referring to null. Let's do a final refactoring to fully remove null from the user defined function.

### Best Scala Style Solution (What about performance?)

We'll use `Option` to get rid of null once and for all!

```
def isEvenOption(n: Integer): Option[Boolean] = {
  val num = Option(n).getOrElse(return None)
  Some(num % 2 == 0)
}

val isEvenOptionUdf = udf[Option[Boolean], Integer](isEvenOption)
```

The `isEvenOption` function converts the integer to an `Option` value and returns `None` if the conversion cannot take place. This code does not use `null` and follows the purist advice: "Ban null from any of your code. Period."

A smart commenter pointed out that returning in the middle of a function is a Scala antipattern and this code is even more elegant:

```
def isEvenOption(n:Int): Option[Boolean] = {
  Option(n).map( _ % 2 == 0)
}
```

Both solution Scala option solutions are less performant than directly referring to null, so a refactoring should be considered if performance becomes a bottleneck.

### User Defined Functions Cannot Take Options as Params

User defined functions surprisingly cannot take an `Option` value as a parameter, so this code won't work:

```
def isEvenBroke(n: Option[Integer]): Option[Boolean] = {
  val num = n.getOrElse(return None)
  Some(num % 2 == 0)
}

val isEvenBrokeUdf = udf[Option[Boolean], Option[Integer]](isEvenBroke)
```

If you run this code, you'll get the following error:

```
org.apache.spark.SparkException: Failed to execute user defined function

Caused by: java.lang.ClassCastException: java.lang.Integer cannot be cast to scala.Option
```

## Spark Rules for Dealing with null

Use native Spark code whenever possible to avoid writing `null` edge case logic

1. If UDFs are needed, follow these rules:

- Scala code should deal with null values gracefully and shouldn't error out if there are null values.
- Scala code should return `None` (or null) for values that are unknown, missing, or irrelevant. DataFrames should also use null for for values that are unknown, missing, or irrelevant.
- Use `Option` in Scala code and fall back on null if `Option` becomes a performance bottleneck.
