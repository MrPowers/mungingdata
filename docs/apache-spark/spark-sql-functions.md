---
title: "Introduction to Spark SQL functions"
date: "2018-09-19"
categories: 
  - "apache-spark"
---

# Introduction to Spark SQL functions

Spark SQL functions make it easy to perform DataFrame analyses.

This post will show you how to use the built-in Spark SQL functions and how to build your own SQL functions.

Make sure to read [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) for a detailed overview of how to use SQL functions in production applications.

## Review of common functions

The Spark SQL functions are stored in the `org.apache.spark.sql.functions` object.

[The documentation page](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) lists all of the built-in SQL functions.

Let's create a DataFrame with a `number` column and use the `factorial` function to append a `number_factorial` column.

```
import org.apache.spark.sql.functions._

val df = Seq(2, 3, 4).toDF("number")

df
  .withColumn("number_factorial", factorial(col("number")))
  .show()
```

```
+------+----------------+
|number|number_factorial|
+------+----------------+
|     2|               2|
|     3|               6|
|     4|              24|
+------+----------------+
```

The `factorial()` function takes a single `Column` argument. The `col()` function, also defined in the `org.apache.spark.sql.functions` object, returns a `Column` object based on the column name.

If Spark implicits are imported (i.e. `import spark.implicits._`), then you can also create a `Column` object with the `$` operator. This code also works.

```
import org.apache.spark.sql.functions._
import spark.implicits._

val df = Seq(2, 3, 4).toDF("number")

df
  .withColumn("number_factorial", factorial($"number"))
  .show()
```

## `lit()` function

The `lit()` function creates a `Column` object out of a literal value. Let's create a DataFrame and use the `lit()` function to append a `spanish_hi` column to the DataFrame.

```
val df = Seq("sophia", "sol", "perro").toDF("word")

df
  .withColumn("spanish_hi", lit("hola"))
  .show()
```

```
+------+----------+
|  word|spanish_hi|
+------+----------+
|sophia|      hola|
|   sol|      hola|
| perro|      hola|
+------+----------+
```

The `lit()` function is especially useful when making boolean comparisons.

## `when()` and `otherwise()` functions

The `when()` and `otherwise()` functions are used for control flow in Spark SQL, similar to `if` and `else` in other programming languages.

Let's create a DataFrame of countries and use some `when()` statements to append a `country` column.

```
val df = Seq("china", "canada", "italy", "tralfamadore").toDF("word")

df
  .withColumn(
    "continent",
    when(col("word") === lit("china"), lit("asia"))
      .when(col("word") === lit("canada"), lit("north america"))
      .when(col("word") === lit("italy"), lit("europe"))
      .otherwise("not sure")
  )
  .show()
```

```
+------------+-------------+
|        word|    continent|
+------------+-------------+
|       china|         asia|
|      canada|north america|
|       italy|       europe|
|tralfamadore|     not sure|
+------------+-------------+
```

Spark lets you cut the `lit()` method calls sometimes and to express code compactly.

```
df
  .withColumn(
    "continent",
    when(col("word") === "china", "asia")
      .when(col("word") === "canada", "north america")
      .when(col("word") === "italy", "europe")
      .otherwise("not sure")
  )
  .show()
```

Here's another example of using `when()` to manage control flow.

```
val df = Seq(10, 15, 25).toDF("age")

df
  .withColumn(
    "life_stage",
    when(col("age") < 13, "child")
      .when(col("age") >= 13 && col("age") <= 18, "teenager")
      .when(col("age") > 18, "adult")
  )
  .show()
```

```
+---+----------+
|age|life_stage|
+---+----------+
| 10|     child|
| 15|  teenager|
| 25|     adult|
+---+----------+
```

## Writing your own SQL function

You can use the built-in Spark SQL functions to build your own SQL functions. Let's create a `lifeStage()` function that takes an age as an argument and returns child, teenager or adult.

```
import org.apache.spark.sql.Column

def lifeStage(col: Column): Column = {
  when(col < 13, "child")
    .when(col >= 13 && col <= 18, "teenager")
    .when(col > 18, "adult")
}
```

Let's use the `lifeStage()` function in a code snippet.

```
val df = Seq(10, 15, 25).toDF("age")

df
  .withColumn(
    "life_stage",
    lifeStage(col("age"))
  )
  .show()
```

```
+---+----------+
|age|life_stage|
+---+----------+
| 10|     child|
| 15|  teenager|
| 25|     adult|
+---+----------+
```

Let's create a `trimUpper()` function that trims all whitespace and capitalizes all of the characters in a string.

```
import org.apache.spark.sql.Column

def trimUpper(col: Column): Column = {
  trim(upper(col))
}
```

Let's run `trimUpper()` on a sample data set.

```
val df = Seq(
  "   some stuff",
  "like CHEESE     "
).toDF("weird")

df
  .withColumn(
    "cleaned",
    trimUpper(col("weird"))
  )
  .show()
```

```
+----------------+-----------+
|           weird|    cleaned|
+----------------+-----------+
|      some stuff| SOME STUFF|
|like CHEESE     |LIKE CHEESE|
+----------------+-----------+
```

Custom SQL functions can typically be used instead of UDFs. Avoiding UDFs is a great way to write better Spark code [as described in this post](https://medium.com/@mrpowers/spark-user-defined-functions-udfs-6c849e39443b).

## Testing SQL functions

You can inspect the SQL that's generated by a SQL function with the `toString` method.

```
lifeStage(lit("10")).toString
```

```
CASE
  WHEN (10 < 13) THEN child
  WHEN ((10 >= 13) AND (10 <= 18)) THEN teenager
  WHEN (10 > 18) THEN adult
END
```

In our test suite, we can make sure that the SQL string that's generated equals what's expected.

```
val expected = "CASE WHEN (10 < 13) THEN child WHEN ((10 >= 13) AND (10 <= 18)) THEN teenager WHEN (10 > 18) THEN adult END"
lifeStage(lit("10")).toString == expected
```

We can also create a DataFrame, append a column with the `lifeStage()` function, and use the [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) library to compare DataFrame equality.

## Next steps

Spark SQL functions are preferable to UDFs because they handle the `null` case gracefully (without a lot of code) and because [they are not a black box](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs-blackbox.html).

Most Spark analyses can be run by leveraging the standard library and reverting to custom SQL functions when necessary. Avoid UDFs at all costs!
