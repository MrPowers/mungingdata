---
title: "Introduction to Spark DataFrames"
date: "2018-09-09"
categories: 
  - "apache-spark"
---

# Introduction to Spark DataFrames

Spark DataFrames are similar to tables in relational databases - they store data in columns and rows and support a variety of operations to manipulate the data.

Here's an example of a DataFrame that contains information about cities.

| city | country | population |
| --- | --- | --- |
| Boston | USA | 0.67 |
| Dubai | UAE | 3.1 |
| Cordoba | Argentina | 1.39 |

This blog post will discuss creating DataFrames, defining schemas, adding columns, and filtering rows.

<iframe width="560" height="315" src="https://www.youtube.com/embed/ATDJF5gXjr0" allowfullscreen></iframe>

## Creating DataFrames

You can import spark implicits and create a DataFrame with the `toDF()` method.

```scala
import spark.implicits._

val df = Seq(
  ("Boston", "USA", 0.67),
  ("Dubai", "UAE", 3.1),
  ("Cordoba", "Argentina", 1.39)
).toDF("city", "country", "population")
```

You can view the contents of a DataFrame with the `show()` method.

```
df.show()
```

```
+-------+---------+----------+
|   city|  country|population|
+-------+---------+----------+
| Boston|      USA|      0.67|
|  Dubai|      UAE|       3.1|
|Cordoba|Argentina|      1.39|
+-------+---------+----------+
```

Each DataFrame column has `name`, `dataType` and `nullable` properties. The column can contain `null` values if the `nullable` property is set to `true`.

The `printSchema()` method provides an easily readable view of the DataFrame schema.

```scala
df.printSchema()
```

```
root
 |-- city: string (nullable = true)
 |-- country: string (nullable = true)
 |-- population: double (nullable = false)
```

## Adding columns

Columns can be added to a DataFrame with the `withColumn()` method.

Let's add an `is_big_city` column to the DataFrame that returns `true` if the city contains more than one million people.

```scala
import org.apache.spark.sql.functions.col

val df2 = df.withColumn("is_big_city", col("population") > 1)
df2.show()
```

```
+-------+---------+----------+-----------+
|   city|  country|population|is_big_city|
+-------+---------+----------+-----------+
| Boston|      USA|      0.67|      false|
|  Dubai|      UAE|       3.1|       true|
|Cordoba|Argentina|      1.39|       true|
+-------+---------+----------+-----------+
```

DataFrames are immutable, so the `withColumn()` method returns a new DataFrame. `withColumn()` does not mutate the original DataFrame. Let's confirm that `df` is still the same with `df.show()`.

```
+-------+---------+----------+
|   city|  country|population|
+-------+---------+----------+
| Boston|      USA|      0.67|
|  Dubai|      UAE|       3.1|
|Cordoba|Argentina|      1.39|
+-------+---------+----------+
```

`df` does not contain the `is_big_city` column, so we've confirmed that `withColumn()` did not mutate `df`.

## Filtering rows

The `filter()` method removes rows from a DataFrame.

```scala
df.filter(col("population") > 1).show()
```

```
+-------+---------+----------+
|   city|  country|population|
+-------+---------+----------+
|  Dubai|      UAE|       3.1|
|Cordoba|Argentina|      1.39|
+-------+---------+----------+
```

It's a little hard to read code with multiple method calls on the same line, so let's break this code up on multiple lines.

```scala
df
  .filter(col("population") > 1)
  .show()
```

We can also assign the filtered DataFrame to a separate variable rather than chaining method calls.

```scala
val filteredDF = df.filter(col("population") > 1)
filteredDF.show()
```

## More on schemas

As previously discussed, the DataFrame schema can be pretty printed to the console with the `printSchema()` method. The `schema` method returns a code representation of the DataFrame schema.

```scala
df.schema
```

```scala
StructType(
  StructField(city, StringType, true),
  StructField(country, StringType, true),
  StructField(population, DoubleType, false)
)
```

Each column of a Spark DataFrame is modeled as a `StructField` object with name, columnType, and nullable properties. The entire DataFrame schema is modeled as a `StructType`, which is a collection of `StructField` objects.

Let's create a schema for a DataFrame that has `first_name` and `age` columns.

```scala
import org.apache.spark.sql.types._

StructType(
  Seq(
    StructField("first_name", StringType, true),
    StructField("age", DoubleType, true)
  )
)
```

Spark's programming interface makes it easy to define the exact schema you'd like for your DataFrames.

## Creating DataFrames with createDataFrame()

The `toDF()` method for creating Spark DataFrames is quick, but it's limited because it doesn't let you define your schema (it infers the schema for you). The `createDataFrame()` method lets you define your DataFrame schema.

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val animalData = Seq(
  Row(30, "bat"),
  Row(2, "mouse"),
  Row(25, "horse")
)

val animalSchema = List(
  StructField("average_lifespan", IntegerType, true),
  StructField("animal_type", StringType, true)
)

val animalDF = spark.createDataFrame(
  spark.sparkContext.parallelize(animalData),
  StructType(animalSchema)
)

animalDF.show()
```

```
+----------------+-----------+
|average_lifespan|animal_type|
+----------------+-----------+
|              30|        bat|
|               2|      mouse|
|              25|      horse|
+----------------+-----------+
```

Read [this blog post](https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393) if you'd like more information on different approaches to create Spark DataFrames.

We can use the `animalDF.printSchema()` method to confirm that the schema was created as specified.

```
root
 |-- average_lifespan: integer (nullable = true)
 |-- animal_type: string (nullable = true)
```

## Next Steps

DataFrames are the fundamental building blocks of Spark. All machine learning and streaming analyses are built on top of the DataFrame API. Make sure you master DataFrames before diving in to more advanced parts of the Spark API.
