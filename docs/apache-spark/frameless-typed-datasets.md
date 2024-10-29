---
title: "Expressively Typed Spark Datasets with Frameless"
date: "2020-10-26"
categories: 
  - "apache-spark"
---

# Expressively Typed Spark Datasets with Frameless

[frameless](https://github.com/typelevel/frameless) is a great library for writing Datasets with expressive types. The library helps users write correct code with descriptive compile time errors instead of runtime errors with long stack traces.

This blog post shows how to build typed datasets with frameless. It demonstrates the improved error messages, explains how to add columns / run functions, and discusses how the library could be improved.

## Create datasets

Let's create a regular Spark dataset using the built-in functions.

```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
import spark.implicits._

case class City(name: String, population: Double)
val cities = Seq(
  City("Manila", 12.8),
  City("Brasilia", 2.5),
  City("Lagos", 14.4)
)
val citiesDS = spark.createDataset(cities)
```

Let's display the dataset contents:

```
citiesDS.show()

+--------+----------+
|    name|population|
+--------+----------+
|  Manila|      12.8|
|Brasilia|       2.5|
|   Lagos|      14.4|
+--------+----------+
```

We can build a typed dataset with the frameless API in a similar manner.

```scala
import frameless.TypedDataset
import frameless.syntax._
val citiesTDS = TypedDataset.create(cities)
```

`citiesDS` is a "regular" Spark dataset and `citiesTDS` is a frameless typed dataset.

Let's display the contents of the typed dataset.

```
citiesTDS.dataset.show()

+--------+----------+
|    name|population|
+--------+----------+
|  Manila|      12.8|
|Brasilia|       2.5|
|   Lagos|      14.4|
+--------+----------+
```

## Selecting a column

Select a column from the Spark Dataset and display the contents to the screen.

```scala
val cities = citiesDS.select("population")
cities.show()

+----------+
|population|
+----------+
|      12.8|
|       2.5|
|      14.4|
+----------+
```

Now select a column from the typed dataset and display the contents to the screen.

```scala
val cities: TypedDataset[Double] = citiesTDS.select(citiesTDS('population))
cities.dataset.show()
```

```
+----+
|  _1|
+----+
|12.8|
| 2.5|
|14.4|
+----+
```

Typed datasets provide better error messages if you try to select columns that are not present. Here's the error message if you try to select a `continent` column from the regular Spark dataset with `citiesDS.select("continent")`.

```
[info]   org.apache.spark.sql.AnalysisException: cannot resolve '`continent`' given input columns: [name, population];;
[info] 'Project ['continent]
[info] +- LocalRelation [name#30, population#31]
[info]   at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
[info]   at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$3.applyOrElse(CheckAnalysis.scala:110)
[info]   at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$3.applyOrElse(CheckAnalysis.scala:107)
[info]   at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:278)
[info]   at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:278)
[info]   at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
[info]   at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:277)
[info]   at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:93)
[info]   at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:93)
[info]   at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:105)
```

This is a runtime error. The Spark Dataset API does not catch this error at compile time.

Trying to access a column that doesn't exist with the frameless API yields a compile error. Here's the error for `citiesTDS.select(DatasetCreator.citiesTDS('continent))`:

```
[info] Compiling 1 Scala source to /Users/powers/Documents/code/my_apps/spark-frameless/target/scala-2.11/test-classes ...
[error] /Users/powers/Documents/code/my_apps/spark-frameless/src/test/scala/mrpowers/spark/frameless/CitiesSpec.scala:36:46: No column Symbol with shapeless.tag.Tagged[String("continent")] of type A in mrpowers.spark.frameless.DatasetCreator.City
[error]     citiesTDS.select(DatasetCreator.citiesTDS('continent))
[error]                                              ^
[error] one error found
[error] (Test / compileIncremental) Compilation failed
```

frameless gives a descriptive compile time error message that's easier to decipher than the standard runtime error.

## Why compile time error messages are better

Compile time errors are better than runtime errors.

Lots of Spark jobs are run with this workflow:

- Write some code
- Compile the code
- Attach the JAR file to a cluster
- Run the code in production

Our previous example demonstrates that the native Spark Dataset API will let you compile code that references columns that aren't in the underlying dataset. You can easily compile code that's not correct and not notice till you run your job in production.

Spark programmers try to minimize the risk of runtime errors with [spark-daria](https://github.com/MrPowers/spark-daria) DataFrame validation checks or by [Testing Spark Applications](https://leanpub.com/testing-spark).

The frameless philosophy is to rely on automated compile time checks rather than manually checking the correctness of all aspects of the program. We can already see how frameless helps you write better code. Let's check out more cool features!

## Adding columns

You can add a column to a typed dataset with `withColumn`, but the entire dataset schema must be supplied.

```scala
import frameless.functions._

case class City2(name: String, population: Double, greeting: String)
val tds2 = citiesTDS.withColumn[City2](lit("hi"))
tds2.dataset.show()
```

```
+--------+----------+--------+
|    name|population|greeting|
+--------+----------+--------+
|  Manila|      12.8|      hi|
|Brasilia|       2.5|      hi|
|   Lagos|      14.4|      hi|
+--------+----------+--------+
```

Supplying an entirely new schema when adding a single column isn't easy, especially for datasets with a lot of columns. You can also add a column with `withColumnTupled`.

```scala
val tds2 = citiesTDS.withColumnTupled(lit("hi"))
tds2.dataset.show()
```

```
+--------+----+---+
|      _1|  _2| _3|
+--------+----+---+
|  Manila|12.8| hi|
|Brasilia| 2.5| hi|
|   Lagos|14.4| hi|
+--------+----+---+
```

You can inspect the schema with `tds2.dataset.printSchema()` to see that all the dataset column names are now \_1, \_2, \_n.

```
root
 |-- _1: string (nullable = false)
 |-- _2: double (nullable = false)
 |-- _3: string (nullable = false)
```

This is far from ideal. We don't want to lose existing column names when adding a new column. Let's hope a new method is added to the API to make it a bit easier to add columns.

[The docs](https://typelevel.org/frameless/FeatureOverview.html) discuss using `asCol` as a potential workaround for this issue.

## Functions

Let's append "is fun" to all the city names.

```scala
import frameless.functions.nonAggregate._

val cities = citiesTDS.select(
  concat(citiesTDS('name), lit(" is fun")),
  citiesTDS('population)
)
cities.dataset.show()
```

Here's what's printed:

```
+---------------+----+
|             _1|  _2|
+---------------+----+
|  Manila is fun|12.8|
|Brasilia is fun| 2.5|
|   Lagos is fun|14.4|
+---------------+----+
```

## Conclusion

frameless is a really cool library that's still being actively developed and is already used by many companies for their production workflows.

Catching errors at compile time is always better than compiling code and dealing with production runtime issues.

See [this repo](https://github.com/MrPowers/spark-frameless) for all the code from this post.

Hopefully a new method will be added to the API that'll make it easier to add columns to a typed dataset.
