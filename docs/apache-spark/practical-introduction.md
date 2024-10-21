---
title: "Practical Introduction to Apache Spark"
date: "2019-01-21"
categories: 
  - "apache-spark"
---

# Practical Introduction to Apache Spark

This book will teach you how to be a proficient Apache Spark programmer with minimal effort.

Other books focus on the theoretical underpinnings of Spark. This book skips the theory and only covers the practical knowledge needed to write production grade Spark code.

## Table of Contents

- [Using the Spark shell](https://www.mungingdata.com/apache-spark/using-the-console)
- [Introduction to DataFrames](https://www.mungingdata.com/apache-spark/introduction-to-dataframes)
- [Just enough Scala for Spark programmers](https://www.mungingdata.com/apache-spark/just-enough-scala)
- [Column methods](https://www.mungingdata.com/apache-spark/column-methods)
- [SQL functions](https://www.mungingdata.com/apache-spark/spark-sql-functions)
- [Creating Spark DataFrames](https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393)
- [Using null in Spark DataFrames](https://www.mungingdata.com/apache-spark/dealing-with-null)
- [User defined functions](https://medium.com/@mrpowers/spark-user-defined-functions-udfs-6c849e39443b)
- [Chaining DataFrame transformations](https://becominghuman.ai/chaining-spark-sql-functions-and-user-defined-functions-2e98534b6885)
- [Schema independent DataFrame transformations](https://medium.com/@mrpowers/schema-independent-dataframe-transformations-d6b36e12dca6)
- [Aggregations](https://www.mungingdata.com/apache-spark/aggregations)
- [Dates / times (DateType, TimestampType)](https://www.mungingdata.com/apache-spark/dates-times)
- [Defining DataFrame Schemas with StructField and StructType](https://www.mungingdata.com/apache-spark/dataframe-schema-structfield-structtype)
- [ArrayType columns](https://www.mungingdata.com/apache-spark/arraytype-columns)
- MapType columns
- [Managing the SparkSession, The DataFrame Entry Point](https://www.mungingdata.com/apache-spark/sparksession)
- CSV file format
- Parquet file format
- JSON file format
- [Managing partitions with repartition and coalesce](https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4)
- Different types of DataFrame joins
- [Broadcast joins](https://www.mungingdata.com/apache-spark/broadcast-joins)
- [Introduction to SBT for Spark programmers](https://www.mungingdata.com/apache-spark/introduction-to-sbt)
- [Building Spark JAR files](https://www.mungingdata.com/apache-spark/building-jar-files-with-sbt)
- [Testing with uTest](https://www.mungingdata.com/apache-spark/testing-with-utest)
- Window functions
- Dependency injection
- Best practices
- [Logistic Regressions](https://www.mungingdata.com/apache-spark/logistic-regressions)
- Extending core classes
- Customizing logs

## Scala programming language

Scala is a vast, multi-paradigm programming language that's notoriously difficult to learn.

Scala can be used as a functional language, an object oriented language, or a mix of both.

You only need to know a small fraction of the Scala programming langauge to be a productive Spark developer. You need to know how to write Scala functions, define packages, and import namespaces. Complex Scala programming topics can be ignored completely.

I knew two programmers that began their Spark learning journey by taking the [Functional Programming Principles in Scala](https://www.coursera.org/learn/progfun1) course by [Martin Odersky](https://en.wikipedia.org/wiki/Martin_Odersky). Odersky created the Scala programming language and is incredibly intelligent. His course is amazing, but very hard, so my friends felt intimidated by Spark. How could they possibly learn Spark if they could barely make it through a beginners course on Scala?

As it turns out, Spark programmers don't need to know anything about advanced Scala language features or functional programming, so courses like [Functional Programming Principles in Scala](https://www.coursera.org/learn/progfun1) are complete overkill.

Check out the [Just enough Scala for Spark programmers](https://www.mungingdata.com/apache-spark/just-enough-scala) post to see just how little Scala is necessary for Spark.

## PySpark or Scala?

Choosing between Python and Scala would normally be a big technology decision (e.g. if you were building a web application), but it's not as important for Spark because the APIs are so similar.

Let's look at some Scala code that adds a column to a DataFrame:

```
import org.apache.spark.sql.functions._

df.withColumn("greeting", lit("hi"))
```

Here's the same code in Python:

```
from pyspark.sql.functions import lit

df.withColumn("greeting", lit("hi"))
```

PySpark code looks a lot like Scala code.

The community is shifting towards PySpark so that's a good place to get started, but it's not a mission critical decision. It's all compiled to Spark at the end of the day!

## Running Spark code

Spinning up your own Spark clusters is complicated. You need to install Spark, create the driver node, create the worker nodes, and make sure messages are properly being sent across machines in the cluster.

[Databricks](https://databricks.com/) let's you easily spin up a cluster with Spark installed, so you don't need to worry about provisioning packages or cluster management. Databricks also has cool features like autoscaling clusters.

If you're just getting started with Spark, it's probably best to pay a bit more and use Databricks.

## Theoretical stuff to ignore

Spark is a big data engine that's built on theoretical cluster computing principles.

You don't need to know how Spark works to solve problems with Spark!

Most books cover a lot of theoretical Spark before teaching the practical basics.

This book aims to provide the easiest possible introduction to Apache Spark by starting with the practical basics. Enjoy!
