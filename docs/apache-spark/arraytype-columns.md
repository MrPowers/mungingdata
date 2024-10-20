---
title: "Working with Spark ArrayType columns"
date: "2019-03-17"
categories: 
  - "apache-spark"
---

Spark DataFrame columns support arrays, which are great for data sets that have an arbitrary length.

This blog post will demonstrate Spark methods that return ArrayType columns, describe how to create your own ArrayType columns, and explain when to use arrays in your analyses.

See [this post](https://mungingdata.com/pyspark/array-arraytype-list/) if you're using Python / PySpark. The rest of this blog uses Scala.

The [Beautiful Spark book](https://leanpub.com/beautiful-spark) is the best way for you to learn about the most important parts of Spark, like ArrayType columns. The book is easy to read and will help you level-up your Spark skills.

## Scala collections

Scala has different types of collections: lists, sequences, and arrays. Let's quickly review the different types of Scala collections before jumping into collections for Spark analyses.

Let's create and sort a collection of numbers.

```
List(10, 2, 3).sorted // List[Int] = List(2, 3, 10)
Seq(10, 2, 3).sorted // Seq[Int] = List(2, 3, 10)
Array(10, 2, 3).sorted // Array[Int] = Array(2, 3, 10)
```

`List`, `Seq`, and `Array` differ slightly, but generally work the same. Most Spark programmers don't need to know about how these collections differ.

Spark uses arrays for `ArrayType` columns, so we'll mainly use arrays in our code snippets.

## Splitting a string into an ArrayType column

Let’s create a DataFrame with a `name` column and a `hit_songs` pipe delimited string. Then let’s use the `split()` method to convert `hit_songs` into an array of strings.

```
val singersDF = Seq(
  ("beatles", "help|hey jude"),
  ("romeo", "eres mia")
).toDF("name", "hit_songs")

val actualDF = singersDF.withColumn(
  "hit_songs",
  split(col("hit_songs"), "\\|")
)
```

```
actualDF.show()

+-------+----------------+
|   name|       hit_songs|
+-------+----------------+
|beatles|[help, hey jude]|
|  romeo|      [eres mia]|
+-------+----------------+
```

```
actualDF.printSchema()

root
 |-- name: string (nullable = true)
 |-- hit_songs: array (nullable = true)
 |    |-- element: string (containsNull = true)
```

An `ArrayType` column is suitable in this example because a singer can have an arbitrary amount of hit songs. We don’t want to create a DataFrame with `hit_song1`, `hit_song2`, …, `hit_songN` columns.

## Directly creating an `ArrayType` column

Let’s use the [spark-daria](https://github.com/MrPowers/spark-daria/) `createDF` method to create a DataFrame with an ArrayType column directly. See [this blog post](https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393) for more information about the `createDF` method.

Let's create another `singersDF` with some different artists.

```
val singersDF = spark.createDF(
  List(
    ("bieber", Array("baby", "sorry")),
    ("ozuna", Array("criminal"))
  ), List(
    ("name", StringType, true),
    ("hit_songs", ArrayType(StringType, true), true)
  )
)
```

```
singersDF.show()

+------+-------------+
|  name|    hit_songs|
+------+-------------+
|bieber|[baby, sorry]|
| ozuna|   [criminal]|
+------+-------------+
```

```
singersDF.printSchema()

root
 |-- name: string (nullable = true)
 |-- hit_songs: array (nullable = true)
 |    |-- element: string (containsNull = true)
```

The `ArrayType` case class is instantiated with an `elementType` and a `containsNull` flag. In `ArrayType(StringType, true)`, `StringType` is the `elementType` and `true` is the `containsNull` flag.

See the documentation for the class [here](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.ArrayType).

## `array_contains`

The Spark [functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) object provides helper methods for working with `ArrayType` columns. The `array_contains` method returns `true` if the column contains a specified element.

Let’s create an array with people and their favorite colors. Then let’s use `array_contains` to append a `likes_red` column that returns true if the person likes red.

```
val peopleDF = spark.createDF(
  List(
    ("bob", Array("red", "blue")),
    ("maria", Array("green", "red")),
    ("sue", Array("black"))
  ), List(
    ("name", StringType, true),
    ("favorite_colors", ArrayType(StringType, true), true)
  )
)

val actualDF = peopleDF.withColumn(
  "likes_red",
  array_contains(col("favorite_colors"), "red")
)
```

```
actualDF.show()

+-----+---------------+---------+
| name|favorite_colors|likes_red|
+-----+---------------+---------+
|  bob|    [red, blue]|     true|
|maria|   [green, red]|     true|
|  sue|        [black]|    false|
+-----+---------------+---------+
```

## `explode`

Let's use the same DataFrame before and the `explode()` to create a new row for every element in each array.

```
val df = peopleDF.select(
  col("name"),
  explode(col("favorite_colors")).as("color")
)
```

```
df.show()

+-----+-----+
| name|color|
+-----+-----+
|  bob|  red|
|  bob| blue|
|maria|green|
|maria|  red|
|  sue|black|
+-----+-----+
```

`peopleDF` has 3 rows and `df` has 5 rows. The `explode()` method adds rows to a DataFrame.

## `collect_list`

The `collect_list` method collapses a DataFrame into fewer rows and stores the collapsed data in an `ArrayType` column.

Let's create a DataFrame with `letter1`, `letter2`, and `number1` columns.

```
val df = Seq(
  ("a", "b", 1),
  ("a", "b", 2),
  ("a", "b", 3),
  ("z", "b", 4),
  ("a", "x", 5)
).toDF("letter1", "letter2", "number1")

df.show()
```

```
+-------+-------+-------+
|letter1|letter2|number1|
+-------+-------+-------+
|      a|      b|      1|
|      a|      b|      2|
|      a|      b|      3|
|      z|      b|      4|
|      a|      x|      5|
+-------+-------+-------+
```

Let's use the `collect_list()` method to eliminate all the rows with duplicate `letter1` and `letter2` rows in the DataFrame and collect all the `number1` entries as a list.

```
df
  .groupBy("letter1", "letter2")
  .agg(collect_list("number1") as "number1s")
  .show()
```

```
+-------+-------+---------+
|letter1|letter2| number1s|
+-------+-------+---------+
|      a|      x|      [5]|
|      z|      b|      [4]|
|      a|      b|[1, 2, 3]|
+-------+-------+---------+
```

We can see that `number1s` is an `ArrayType` column.

```
df.printSchema

root
 |-- letter1: string (nullable = true)
 |-- letter2: string (nullable = true)
 |-- number1s: array (nullable = true)
 |    |-- element: integer (containsNull = true)
```

## Single column array functions

Spark added a ton of useful array functions in [the 2.4 release](https://databricks.com/blog/2018/11/16/introducing-new-built-in-functions-and-higher-order-functions-for-complex-data-types-in-apache-spark.html).

We will start with the functions for a single `ArrayType` column and then move on to the functions for multiple `ArrayType` columns.

Let's start by creating a DataFrame with an `ArrayType` column.

```
val df = spark.createDF(
  List(
    (Array(1, 2)),
    (Array(1, 2, 3, 1)),
    (null)
  ), List(
    ("nums", ArrayType(IntegerType, true), true)
  )
)
```

```
df.show()

+------------+
|        nums|
+------------+
|      [1, 2]|
|[1, 2, 3, 1]|
|        null|
+------------+
```

Let's use the `array_distinct()` method to remove all of the duplicate array elements in the `nums` column.

```
df
  .withColumn("nums_distinct", array_distinct($"nums"))
  .show()

+------------+-------------+
|        nums|nums_distinct|
+------------+-------------+
|      [1, 2]|       [1, 2]|
|[1, 2, 3, 1]|    [1, 2, 3]|
|        null|         null|
+------------+-------------+
```

Let's use `array_join()` to create a pipe delimited string of all elements in the arrays.

```
df
  .withColumn("nums_joined", array_join($"nums", "|"))
  .show()

+------------+-----------+
|        nums|nums_joined|
+------------+-----------+
|      [1, 2]|        1|2|
|[1, 2, 3, 1]|    1|2|3|1|
|        null|       null|
+------------+-----------+
```

Let's use the `printSchema` method to verify that the `nums_joined` column is a `StringType`.

```
df
  .withColumn("nums_joined", array_join($"nums", "|"))
  .printSchema()

root
 |-- nums: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- nums_joined: string (nullable = true)
```

Let's use `array_max` to grab the maximum value from the arrays.

```
df
  .withColumn("nums_max", array_max($"nums"))
  .show()

+------------+--------+
|        nums|nums_max|
+------------+--------+
|      [1, 2]|       2|
|[1, 2, 3, 1]|       3|
|        null|    null|
+------------+--------+
```

Let's use `array_min` to grab the minimum value from the arrays.

```
df
  .withColumn("nums_min", array_min($"nums"))
  .show()

+------------+--------+
|        nums|nums_min|
+------------+--------+
|      [1, 2]|       1|
|[1, 2, 3, 1]|       1|
|        null|    null|
+------------+--------+
```

Let's use the `array_remove` method to remove all the 1s from each of the arrays.

```
df
  .withColumn("nums_sans_1", array_remove($"nums", 1))
  .show()

+------------+-----------+
|        nums|nums_sans_1|
+------------+-----------+
|      [1, 2]|        [2]|
|[1, 2, 3, 1]|     [2, 3]|
|        null|       null|
+------------+-----------+
```

Let's use `array_sort` to sort all of the arrays in ascending order.

```
df
  .withColumn("nums_sorted", array_sort($"nums"))
  .show()

+------------+------------+
|        nums| nums_sorted|
+------------+------------+
|      [1, 2]|      [1, 2]|
|[1, 2, 3, 1]|[1, 1, 2, 3]|
|        null|        null|
+------------+------------+
```

## Spark 3 Array Functions

Spark 3 added some incredibly useful array functions [as described in this post](https://mungingdata.com/spark-3/array-exists-forall-transform-aggregate-zip_with/).

exists, forall, transform, aggregate, and zip\_with makes it much easier to use ArrayType columns with native Spark code instead of using UDFs. Make sure to read [the blog post](https://mungingdata.com/spark-3/array-exists-forall-transform-aggregate-zip_with/) that discusses these functions in detail if you're using Spark 3.

## Generic single column array functions

Skip this section if you're using Spark 3. The approach outlined in this section is only needed for Spark 2.

Suppose you have an array of strings and would like to see if all elements in the array begin with the letter `c`. Here's how you can run this check on a Scala array:

```
Array("cream", "cookies").forall(_.startsWith("c")) // true
Array("taco", "clam").forall(_.startsWith("c")) // false
```

You can use the [spark-daria](https://github.com/MrPowers/spark-daria) `forall()` method to run this computation on a Spark DataFrame with an `ArrayType` column.

```
import com.github.mrpowers.spark.daria.sql.functions._

val df = spark.createDF(
  List(
    (Array("cream", "cookies")),
    (Array("taco", "clam"))
  ), List(
    ("words", ArrayType(StringType, true), true)
  )
)

df.withColumn(
  "all_words_begin_with_c",
  forall[String]((x: String) => x.startsWith("c")).apply(col("words"))
).show()
```

```
+----------------+----------------------+
|           words|all_words_begin_with_c|
+----------------+----------------------+
|[cream, cookies]|                  true|
|    [taco, clam]|                 false|
+----------------+----------------------+
```

The native Spark API doesn't provide access to all the helpful collection methods provided by Scala. [spark-daria](https://github.com/MrPowers/spark-daria) uses User Defined Functions to define `forall` and `exists` methods. [Email me](https://github.com/MrPowers) or create an issue if you would like any additional UDFs to be added to spark-daria.

## Multiple column array functions

Let's create a DataFrame with two `ArrayType` columns so we can try out the built-in Spark array functions that take multiple columns as input.

```
val numbersDF = spark.createDF(
  List(
    (Array(1, 2), Array(4, 5, 6)),
    (Array(1, 2, 3, 1), Array(2, 3, 4)),
    (null, Array(6, 7))
  ), List(
    ("nums1", ArrayType(IntegerType, true), true),
    ("nums2", ArrayType(IntegerType, true), true)
  )
)
```

Let's use `array_intersect` to get the elements present in both the arrays without any duplication.

```
numbersDF
  .withColumn("nums_intersection", array_intersect($"nums1", $"nums2"))
  .show()

+------------+---------+-----------------+
|       nums1|    nums2|nums_intersection|
+------------+---------+-----------------+
|      [1, 2]|[4, 5, 6]|               []|
|[1, 2, 3, 1]|[2, 3, 4]|           [2, 3]|
|        null|   [6, 7]|             null|
+------------+---------+-----------------+
```

Let's use `array_union` to get the elements in either array, without duplication.

```
numbersDF
  .withColumn("nums_union", array_union($"nums1", $"nums2"))
  .show()
```

```
+------------+---------+---------------+
|       nums1|    nums2|     nums_union|
+------------+---------+---------------+
|      [1, 2]|[4, 5, 6]|[1, 2, 4, 5, 6]|
|[1, 2, 3, 1]|[2, 3, 4]|   [1, 2, 3, 4]|
|        null|   [6, 7]|           null|
+------------+---------+---------------+
```

Let's use `array_except` to get the elements that are in `num1` and not in `num2` without any duplication.

```
numbersDF
  .withColumn("nums1_nums2_except", array_except($"nums1", $"nums2"))
  .show()

+------------+---------+------------------+
|       nums1|    nums2|nums1_nums2_except|
+------------+---------+------------------+
|      [1, 2]|[4, 5, 6]|            [1, 2]|
|[1, 2, 3, 1]|[2, 3, 4]|               [1]|
|        null|   [6, 7]|              null|
+------------+---------+------------------+
```

## Split array column into multiple columns

We can split an array column into multiple columns with `getItem`. Lets create a DataFrame with a `letters` column and demonstrate how this single `ArrayType` column can be split into a DataFrame with three `StringType` columns.

```
val df = spark.createDF(
  List(
    (Array("a", "b", "c")),
    (Array("d", "e", "f")),
    (null)
  ), List(
    ("letters", ArrayType(StringType, true), true)
  )
)
```

```
df.show()

+---------+
|  letters|
+---------+
|[a, b, c]|
|[d, e, f]|
|     null|
+---------+
```

This example uses the same data as [this Stackoverflow question](https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala).

Let's use `getItem` to break out the array into `col1`, `col2`, and `col3`.

```
df
  .select(
    $"letters".getItem(0).as("col1"),
    $"letters".getItem(1).as("col2"),
    $"letters".getItem(2).as("col3")
  )
  .show()

+----+----+----+
|col1|col2|col3|
+----+----+----+
|   a|   b|   c|
|   d|   e|   f|
|null|null|null|
+----+----+----+
```

Here's how we can use `getItem` with a loop.

```
df
  .select(
    (0 until 3).map(i => $"letters".getItem(i).as(s"col$i")): _*
  )
  .show()

+----+----+----+
|col0|col1|col2|
+----+----+----+
|   a|   b|   c|
|   d|   e|   f|
|null|null|null|
+----+----+----+
```

Our code snippet above is a little ugly because the 3 is hardcoded. We can calculate the size of every array in the column, take the max size, and use that rather than hardcoding.

```
val numCols = df
  .withColumn("letters_size", size($"letters"))
  .agg(max($"letters_size"))
  .head()
  .getInt(0)

df
  .select(
    (0 until numCols).map(i => $"letters".getItem(i).as(s"col$i")): _*
  )
  .show()

+----+----+----+
|col0|col1|col2|
+----+----+----+
|   a|   b|   c|
|   d|   e|   f|
|null|null|null|
+----+----+----+
```

## Complex Spark Column types

Spark supports [MapType](https://mungingdata.com/apache-spark/maptype-columns/) and [StructType](https://mungingdata.com/apache-spark/dataframe-schema-structfield-structtype/) columns in addition to the ArrayType columns covered in this post.

Check out [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) for a detailed overview of the different complex column types and how they should be used when architecting Spark applications.

## Closing thoughts

Spark `ArrayType` columns makes it easy to work with collections at scale.

Master the content covered in this blog to add a powerful skill to your toolset.
