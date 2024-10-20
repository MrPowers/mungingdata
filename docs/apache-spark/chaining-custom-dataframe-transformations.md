---
title: "Chaining Custom DataFrame Transformations in Spark"
date: "2017-01-27"
categories: 
  - "apache-spark"
---

`implicit classes` or the `Dataset#transform` method can be used to chain DataFrame transformations in Spark. This blog post will demonstrate how to chain DataFrame transformations and explain why the `Dataset#transform` method is preferred compared to `implicit classes`.

Structuring Spark code as DataFrame transformations separates strong Spark programmers from "spaghetti hackers" as detailed in [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/). Following the blog post will make your Spark code much easier to test and reuse.

If you’re using PySpark, [see this article on chaining custom PySpark DataFrame transformations](https://mungingdata.com/pyspark/chaining-dataframe-transformations/).

## Dataset Transform Method

The Dataset transform method provides a “[concise syntax for chaining custom transformations](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset).”

Suppose we have a `withGreeting()` method that appends a greeting column to a DataFrame and a `withFarewell()` method that appends a farewell column to a DataFrame.

```
def withGreeting(df: DataFrame): DataFrame = {
  df.withColumn("greeting", lit("hello world"))
}

def withFarewell(df: DataFrame): DataFrame = {
  df.withColumn("farewell", lit("goodbye"))
}
```

We can use the transform method to run the `withGreeting()` and `withFarewell()` methods.

```
val df = Seq(
  "funny",
  "person"
).toDF("something")

val weirdDf = df
  .transform(withGreeting)
  .transform(withFarewell)
```

```
weirdDf.show()

+---------+-----------+--------+
|something|   greeting|farewell|
+---------+-----------+--------+
|    funny|hello world| goodbye|
|   person|hello world| goodbye|
+---------+-----------+--------+
```

The transform method can easily be chained with built-in Spark DataFrame methods, like select.

```
df
  .select("something")
  .transform(withGreeting)
  .transform(withFarewell)
```

If the transform method is not used then we need to nest method calls and the code becomes less readable.

```
withFarewell(withGreeting(df))

// even worse
withFarewell(withGreeting(df)).select("something")
```

## Transform Method with Arguments

Custom DataFrame transformations that take arguments can also use the transform method by leveraging currying / multiple parameter lists in Scala.

Let’s use the same `withGreeting()` method from earlier and add a `withCat()` method that takes a string as an argument.

```
def withGreeting(df: DataFrame): DataFrame = {
  df.withColumn("greeting", lit("hello world"))
}

def withCat(name: String)(df: DataFrame): DataFrame = {
  df.withColumn("cats", lit(s"$name meow"))
}
```

We can use the transform method to run the `withGreeting()` and `withCat()` methods.

```
val df = Seq(
  "funny",
  "person"
).toDF("something")

val niceDf = df
  .transform(withGreeting)
  .transform(withCat("puffy"))
```

```
niceDf.show()

+---------+-----------+----------+
|something|   greeting|      cats|
+---------+-----------+----------+
|    funny|hello world|puffy meow|
|   person|hello world|puffy meow|
+---------+-----------+----------+
```

The transform method can be used for custom DataFrame transformations that take arguments as well!

## Monkey Patching with Implicit Classes

Implicit classes can be used to add methods to existing classes. The following code adds the same `withGreeting()` and `withFarewell()` methods to the DataFrame class itself.

```
object BadImplicit {

  implicit class DataFrameTransforms(df: DataFrame) {

    def withGreeting(): DataFrame = {
      df.withColumn("greeting", lit("hello world"))
    }

    def withFarewell(): DataFrame = {
      df.withColumn("farewell", lit("goodbye"))
    }

  }

}
```

The `withGreeting()` and `withFarewell()` methods can be chained and executed as follows.

```
import BadImplicit._

val df = Seq(
  "funny",
  "person"
).toDF("something")

val hiDf = df.withGreeting().withFarewell()
```

Extending core classes works, but it's is poor programming practice that should be avoided.

## Avoiding Implicit Classes

> Changing base classes is known as monkey patching and is a delightful feature of Ruby but can be perilous in untutored hands. — Sandi Metz

Sandi’s comment was aimed at the Ruby programming language, but the same principle applies to Scala implicit classes.

Monkey patching in [generally frowned upon](https://www.rubypigeon.com/posts/4-ways-to-avoid-monkey-patching/) in the Ruby community and should be discouraged in Scala.

Spark was nice enough to provide a transform method so you don’t need to monkey patch the DataFrame class. With some clever Scala programming, we can even make the transform method work with custom transformations that take arguments. This makes the transform method the clear winner!
