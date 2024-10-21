---
title: "Managing the SparkSession, The DataFrame Entry Point"
date: "2019-04-09"
categories: 
  - "apache-spark"
---

# Managing the SparkSession, The DataFrame Entry Point

The SparkSession is used to create and read DataFrames. It's used whenever you create a DataFrame in your test suite or whenever you read a Parquet / CSV data lake into a DataFrame.

This post explains how to create a SparkSession, share it throughout your program, and use it to create DataFrames.

## Accessing the SparkSession

A SparkSession is automatically created and stored in the `spark` variable whenever you start the Spark console or open a Databricks notebook.

Your program should reuse the same SparkSession and you should avoid any code that creates and uses a different SparkSession.

## Creating a RDD

Let's open the Spark console and use the `spark` variable to create a RDD from a sequence.

Notice that the message `Spark session available as 'spark'` is printed when you start the Spark shell.

```
val data = Seq(2, 4, 6)
val myRDD = spark.sparkContext.parallelize(data)
```

The `SparkSession` is used to access the `SparkContext`, which has a `parallelize` method that converts a sequence into a RDD.

RDDs aren't used much now that the DataFrame API has been released, but they're still useful when creating DataFrames.

## Creating a DataFrame

The SparkSession is used twice when manually creating a DataFrame:

1. Converts a sequence into a RDD
2. Converts a RDD into a DataFrame

```
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val rdd = spark.sparkContext.parallelize(
  Seq(
    Row("bob", 55)
  )
)

val schema = StructType(
  Seq(
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  )
)

val df = spark.createDataFrame(rdd, schema)
```

```
df.show()

+----+---+
|name|age|
+----+---+
| bob| 55|
+----+---+
```

You will frequently use the SparkSession to create DataFrames when testing your code.

## Reading a DataFrame

The SparkSession is also used to read CSV, JSON, and Parquet files.

Here are some examples.

```
val df1 = spark.read.csv("/mnt/my-bucket/csv-data")
val df2 = spark.read.json("/mnt/my-bucket/json-data")
val df3 = spark.read.parquet("/mnt/my-bucket/parquet-data")
```

There are separate posts on CSV, JSON, and Parquet files that do deep dives into the intracacies of each file format.

## Creating a SparkSession

You can create a SparkSession in your applications with the `getOrCreate` method:

```
val spark = SparkSession.builder().master("local").appName("my cool app").getOrCreate()
```

You don't need to manually create a SparkSession in programming environments that already define the variable (e.g. the Spark shell or a Databricks notebook). Creating your own SparkSession becomes vital when you start writing Spark code in a text editor.

A lot of Spark programmers have trouble writing high quality code because they cannot make the jump to writing Spark code in a text editor with tests. It's hard to write good code without following best practices.

Wrapping the `spark` variable in a `trait` is the best way to share it across different classes and objects in your codebase.

```
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("my cool app").getOrCreate()
  }

}
```

The `getOrCreate()` method will create a new SparkSession if one does not exist, but reuse an exiting SparkSession if it exists.

When your test suite is run, this code will create a `SparkSession` when the first `spark` variable is found. After the initial SparkSession is created, it will be reused for every subsequent reference to `spark`.

Your production environment will probably already define the `spark` variable, so `getOrCreate()` won't ever both creating a SparkSession and will simply use the SparkSession already created by the environment.

Here is how the `SparkSessionWrapper` can be used in some example objects.

```
object transformations extends SparkSessionWrapper {

  def withSomeDatamart(
    coolDF: DataFrame = spark.read.parquet("/mnt/my-bucket/cool-data")
  )(df: DataFrame): DataFrame = {
    df.join(
      broadcast(coolDF),
      df("some_id") <=> coolDF("some_id")
    )
  }

}
```

The `transformations.withSomeDatamart()` method is injecting `coolDF`, so the code can easily be tested and intelligently grab the right file by default when run in production.

Notice how the `spark` variable is used to set our smart default.

We will use the `SparkSessionWrapper` trait and `spark` variable again when testing the `withSomeDatamart` method.

```
import utest._

object TransformsTest extends TestSuite with SparkSessionWrapper with ColumnComparer {

  val tests = Tests {

    'withSomeDatamart - {

      val coolDF = spark.createDF(
        List(

        ), List(

        )
      )

      val df = spark.createDF(
        List(

        ), List(

        )
      ).transform(transformations.withSomeDatamart())

    }

  }

}
```

The test leverages the `createDF` method, which is a SparkSession extension defined in `spark-daria`.

`createDF` is similar to `createDataFrame`, but more cocise. See this blog post on manually creating Spark DataFrames for more details.

## Reusing the SparkSession in the test suite

Starting and stopping the SparkSession is slow, so you want to reuse the same SparkSession throughout your test suite. Don't restart the SparkSession for every test file that is run - Spark tests run slowly enough as is and shouldn't be made any slower.

The `SparkSessionWrapper` can be reused in your application code and the test suite.

## SparkContext

The `SparkSession` encapsulates the `SparkConf`, `SparkContext`, and `SQLContext`.

Prior to Spark 2.0, developers needed to explicly create `SparkConf`, `SparkContext`, and `SQLContext` objects. Now Spark developers, can just create a `SparkSession` and access the other objects as needed.

The following code snippet uses the `SparkSession` to access the `sparkContext`, so the `parallelize` method can be used to create a DataFrame (we saw this same snippet earlier in the blog post).

```
spark.sparkContext.parallelize(
  Seq(
    Row("bob", 55)
  )
)
```

You shouldn't have to access the `sparkContext` much - pretty much only when manually creating DataFrames. See the [spark-daria](https://github.com/MrPowers/spark-daria) `createDF()` method, so you don't even need to explicitly call `sparkContext` when you want to create a DataFrame.

Read [this blog post](https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html) for more information.

## Conclusion

You'll need a SparkSession in your programs to create DataFrames.

Reusing the SparkSession in your application is critical for good code organization. Reusing the SparkSession in your test suite is vital to make your tests execute as quickly as possible.
