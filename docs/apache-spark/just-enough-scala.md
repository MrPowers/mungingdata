---
title: "Just Enough Scala for Spark Programmers"
date: "2018-09-29"
categories: 
  - "apache-spark"
---

Spark programmers only need to know a small subset of the Scala API to be productive.

Scala has a reputation for being a difficult language to learn and that scares some developers away from Spark. This guide covers the Scala language features needed for Spark programmers.

Spark programmers need to know how to write Scala functions, encapsulate functions in objects, and namespace objects in packages. It's not a lot to learn - I promise!

## Scala function basics

This section describes how to write vanilla Scala functions and [Spark SQL functions](https://www.mungingdata.com/apache-spark/spark-sql-functions).

Here is a Scala function that adds two numbers:

```
def sum(num1: Int, num2: Int): Int = {
  num1 + num2
}
```

We can invoke this function as follows:

```
sum(10, 5) // returns 15
```

Let's write a [Spark SQL function](https://www.mungingdata.com/apache-spark/spark-sql-functions) that adds two numbers together:

```
import org.apache.spark.sql.Column

def sumColumns(num1: Column, num2: Column): Column = {
  num1 + num2
}
```

Let's create a DataFrame in [the Spark shell](https://www.mungingdata.com/apache-spark/using-the-console) and run the `sumColumns()` function.

```
val numbersDF = Seq(
  (10, 4),
  (3, 4),
  (8, 4)
).toDF("some_num", "another_num")

numbersDF
  .withColumn(
    "the_sum",
    sumColumns(col("some_num"), col("another_num"))
  )
  .show()
```

```
+--------+-----------+-------+
|some_num|another_num|the_sum|
+--------+-----------+-------+
|      10|          4|     14|
|       3|          4|      7|
|       8|          4|     12|
+--------+-----------+-------+
```

Spark SQL functions take `org.apache.spark.sql.Column` arguments whereas vanilla Scala functions take native Scala data type arguments like `Int` or `String`.

## Currying functions

Scala allows for functions to take multiple parameter lists, which is formally known as currying. This section explains how to use currying with vanilla Scala functions and why currying is important for Spark programmers.

```
def myConcat(word1: String)(word2: String): String = {
  word1 + word2
}
```

Here's how to invoke the `myConcat()` function.

```
myConcat("beautiful ")("picture") // returns "beautiful picture"
```

`myConcat()` is invoked with two sets of arguments.

Spark has a `Dataset#transform()` method that makes it easy to [chain DataFrame transformations](https://medium.com/@mrpowers/chaining-custom-dataframe-transformations-in-spark-a39e315f903c).

Here's an example of a DataFrame transformation function:

```
import org.apache.spark.sql.DataFrame

def withCat(name: String)(df: DataFrame): DataFrame = {
  df.withColumn("cat", lit(s"$name meow"))
}
```

DataFrame transformation functions can take an arbitrary number of arguments in the first parameter list and must take a single DataFrame argument in the second parameter list.

Let's create a DataFrame in [the Spark shell](https://www.mungingdata.com/apache-spark/using-the-console) and run the `withCat()` function.

```
val stuffDF = Seq(
  ("chair"),
  ("hair"),
  ("bear")
).toDF("thing")

stuffDF
  .transform(withCat("darla"))
  .show()
```

```
+-----+----------+
|thing|       cat|
+-----+----------+
|chair|darla meow|
| hair|darla meow|
| bear|darla meow|
+-----+----------+
```

Most Spark code can be organized as Spark SQL functions or as custom DataFrame transformations.

## `object`

Spark functions can be stored in objects.

Let's create a `SomethingWeird` object that defines a vanilla Scala function, a Spark SQL function, and a custom DataFrame transformation.

```
import org.apache.spark.sql.functions._

object SomethingWeird {

  // vanilla Scala function
  def hi(): String = {
    "welcome to planet earth"
  }

  // Spark SQL function
  def trimUpper(col: Column) = {
    trim(upper(col))
  }

  // custom DataFrame transformation
  def withScary()(df: DataFrame): DataFrame = {
    df.withColumn("scary", lit("boo!"))
  }

}
```

Let's create a DataFrame in the Spark shell and run the `trimUpper()` and `withScary()` functions.

```
val wordsDF = Seq(
  ("niCE"),
  ("  CaR"),
  ("BAR  ")
).toDF("word")

wordsDF
  .withColumn("trim_upper_word", SomethingWeird.trimUpper(col("word")))
  .transform(SomethingWeird.withScary())
  .show()
```

```
+-----+---------------+-----+
| word|trim_upper_word|scary|
+-----+---------------+-----+
| niCE|           NICE| boo!|
|  CaR|            CAR| boo!|
|BAR  |            BAR| boo!|
+-----+---------------+-----+
```

Objects are useful for grouping related Spark functions.

## `trait`

Traits can be mixed into objects to add commonly used methods or values. We can define a `SparkSessionWrapper` `trait` that defines a `spark` variable to give objects easy access to the `SparkSession` object.

```
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}
```

The `Serializable` trait is mixed into the `SparkSessionWrapper` trait.

Let's create a `SpecialDataLake` object that mixes in the `SparkSessionWrapper` trait to provide easy access to a data lake.

```
object SpecialDataLake extends SparkSessionWrapper {

  def dataLake(): DataFrame = {
    spark.read.parquet("some_secret_s3_path")
  }

}
```

## `package`

Packages are used to namespace Scala code. Per the [Databricks Scala style guide](https://github.com/databricks/scala-style-guide#naming-convention), packages should follow Java naming conventions.

For example, the Databricks [spark-redshift](https://github.com/databricks/spark-redshift) project uses the `com.databricks.spark.redshift` namespace.

The Spark project used the `org.apache.spark` namespace. [spark-daria](https://github.com/MrPowers/spark-daria) uses the `com.github.mrpowers.spark.daria` namespace.

Here an example of code that's defined in a package in `spark-daria`:

```
package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object functions {

  def singleSpace(col: Column): Column = {
    trim(regexp_replace(col, " +", " "))
  }

}
```

The package structure should mimic the file structure of the project.

## Implicit classes

Implicit classes can be used to extend Spark core classes with additional methods.

Let's add a `lower()` method to the `Column` class that converts all the strings in a column to lower case.

```
package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column

object FunctionsAsColumnExt {

  implicit class ColumnMethods(col: Column) {

    def lower(): Column = {
      org.apache.spark.sql.functions.lower(col)
    }

  }

}
```

After running `import com.github.mrpowers.spark.daria.sql.FunctionsAsColumnExt._`, you can run the `lower()` method directly on column objects.

```
col("some_string").lower()
```

Implicit classes should be avoided in general. I only monkey patch core classes in the [spark-daria](https://github.com/MrPowers/spark-daria/) project. Feel free to send pull requests if you have any good ideas for other extensions.

## Next steps

There are a couple of other Scala features that are useful when writing Spark code, but this blog post covers 90%+ of common use cases.

You don't need to understand functional programming or advanced Scala language features to be a productive Spark programmer.

In fact, staying away from UDFs and native Scala code is [a best practice](https://medium.com/@mrpowers/spark-user-defined-functions-udfs-6c849e39443b).

Focus on mastering the native Spark API and you'll be a productive big data engineer in no time!
