---
title: "Exploring Spark's Column Methods"
date: "2019-02-19"
categories: 
  - "apache-spark"
---

# Exploring Spark's Column Methods

The Spark [Column class](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column) defines a variety of column methods that are vital for manipulating DataFrames.

This blog post demonstrates how to instantiate Column objects and covers the commonly used Column methods.

<iframe width="560" height="315" src="https://www.youtube.com/embed/gj9g8QSyssQ" allowfullscreen></iframe>

## A simple example

Let's create a little DataFrame with superheros and their city of origin.

```scala
val df = Seq(
  ("thor", "new york"),
  ("aquaman", "atlantis"),
  ("wolverine", "new york")
).toDF("superhero", "city")
```

Let's use the `startsWith()` column method to identify all cities that start with the word `new`:

```scala
df
  .withColumn("city_starts_with_new", $"city".startsWith("new"))
  .show()
```

```
+---------+--------+--------------------+
|superhero|    city|city_starts_with_new|
+---------+--------+--------------------+
|     thor|new york|                true|
|  aquaman|atlantis|               false|
|wolverine|new york|                true|
+---------+--------+--------------------+
```

A Column object is instantiated with the `$"city"` statement. Let's look at all the different ways to create Column objects.

## Instantiating Column objects

Column objects must be created to run Column methods.

A Column object corresponding with the `city` column can be created using the following three syntaxes:

1. `$"city"`
2. `df("city")`
3. `col("city")` (must run `import org.apache.spark.sql.functions.col` first)

Column objects are commonly passed as arguments to SQL functions (e.g. `upper($"city")`).

We will create column objects in all the examples that follow.

## `gt()`

Let's create a DataFrame with an integer column so we can run some numerical column methods.

```scala
val df = Seq(
  (10, "cat"),
  (4, "dog"),
  (7, null)
).toDF("num", "word")
```

Let's use the `gt()` method to identify all rows with a `num` greater than five.

```scala
df
  .withColumn("num_gt_5", col("num").gt(5))
  .show()
```

```
+---+----+--------+
|num|word|num_gt_5|
+---+----+--------+
| 10| cat|    true|
|  4| dog|   false|
|  7|null|    true|
+---+----+--------+
```

## `substr()`

Let's use the `substr()` method to create a new column with the first two letters of the `word` column.

```scala
df
  .withColumn("word_first_two", col("word").substr(0, 2))
  .show()
```

```
+---+----+--------------+
|num|word|word_first_two|
+---+----+--------------+
| 10| cat|            ca|
|  4| dog|            do|
|  7|null|          null|
+---+----+--------------+
```

Notice that the `substr()` method returns `null` when it's supplied `null` as input. All other Column methods and SQL functions behave similarly (i.e. they return `null` when the input is `null`).

## `+` operator

Let's use the `+` operator to add five to the `num` column.

```scala
df
  .withColumn("num_plus_five", col("num").+(5))
  .show()
```

```
+---+----+-------------+
|num|word|num_plus_five|
+---+----+-------------+
| 10| cat|           15|
|  4| dog|            9|
|  7|null|           12|
+---+----+-------------+
```

We can also skip the dot notation when invoking the function.

```scala
df
  .withColumn("num_plus_five", col("num") + 5)
  .show()
```

The syntactic sugar makes it harder to see that `+` is a method defined in the Column class. Take a look at [the docs](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column) to convince yourself!

## `lit()`

Let's use the `/` method to take two divided by the `num` column.

```scala
df
  .withColumn("two_divided_by_num", lit(2) / col("num"))
  .show()
```

```
+---+----+------------------+
|num|word|two_divided_by_num|
+---+----+------------------+
| 10| cat|               0.2|
|  4| dog|               0.5|
|  7|null|0.2857142857142857|
+---+----+------------------+
```

Notice that the `lit()` function must be used to convert two into a Column object before the division can take place.

```scala
df
  .withColumn("two_divided_by_num", 2 / col("num"))
  .show()
```

Here is the error message:

```
notebook:2: error: overloaded method value / with alternatives:
  (x: Double)Double <and>
  (x: Float)Float <and>
  (x: Long)Long <and>
  (x: Int)Int <and>
  (x: Char)Int <and>
  (x: Short)Int <and>
  (x: Byte)Int
 cannot be applied to (org.apache.spark.sql.Column)
  .withColumn("two_divided_by_num", 2 / col("num"))
```

The `/` method is defined in both the Scala Int and Spark Column classes. We need to convert the number to a Column object, so the compiler knows to use the `/` method defined in the Spark Column class. Upon analyzing the error message, we can see that the compiler is mistakenly trying to use the `/` operator defined in the Scala Int class.

## `isNull`

Let's use the `isNull` method to identify the rows with a `word` of `null`.

```scala
df
  .withColumn("word_is_null", col("word").isNull)
  .show()
```

```
+---+----+------------+
|num|word|word_is_null|
+---+----+------------+
| 10| cat|       false|
|  4| dog|       false|
|  7|null|        true|
+---+----+------------+
```

## `isNotNull`

Let's use the `isNotNull` method to filter out all rows with a `word` of `null`.

```scala
df
  .where(col("word").isNotNull)
  .show()
```

```
+---+----+
|num|word|
+---+----+
| 10| cat|
|  4| dog|
+---+----+
```

## `when` / `otherwise`

Let's create a final DataFrame with `word1` and `word2` columns, so we can play around with the `===`, `when()`, and `otherwise()` methods

```scala
val df = Seq(
  ("bat", "bat"),
  ("snake", "rat"),
  ("cup", "phone"),
  ("key", null)
).toDF("word1", "word2")
```

Let's write a little word comparison algorithm that analyzes the differences between the two words.

```scala
import org.apache.spark.sql.functions._

df
  .withColumn(
    "word_comparison",
    when($"word1" === $"word2", "same words")
      .when(length($"word1") > length($"word2"), "word1 is longer")
      .otherwise("i am confused")
  ).show()
```

```
+-----+-----+---------------+
|word1|word2|word_comparison|
+-----+-----+---------------+
|  bat|  bat|     same words|
|snake|  rat|word1 is longer|
|  cup|phone|  i am confused|
|  key| null|  i am confused|
+-----+-----+---------------+
```

`when()` and `otherwise()` are how to write `if` / `else if` / `else` logic in Spark.

## Next steps

Spark's Colum methods are frequently used and mastery of this class is vital.

Scala syntactic sugar can make it difficult for programmers without a lot of object oriented experience to have difficulty identifying when Column methods are invoked (and differenting them from SQL functions).

Keep reading though this post till you've mastered all these vital concepts!
