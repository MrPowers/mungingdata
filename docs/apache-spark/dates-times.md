---
title: "Working with dates and times in Spark"
date: "2018-12-22"
categories: 
  - "apache-spark"
---

# Working with dates and times in Spark

Spark supports `DateType` and `TimestampType` columns and defines a rich API of functions to make working with dates and times easy. This blog post will demonstrates how to make DataFrames with `DateType` / `TimestampType` columns and how to leverage Spark's functions for working with these columns.

## Complex Spark Column types

Spark supports [ArrayType](https://mungingdata.com/apache-spark/arraytype-columns/), [MapType](https://mungingdata.com/apache-spark/maptype-columns/) and [StructType](https://mungingdata.com/apache-spark/dataframe-schema-structfield-structtype/) columns in addition to the DateType / TimestampType columns covered in this post.

Check out [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) for a detailed overview of the different complex column types and how they should be used when architecting Spark applications.

## Creating DateType columns

Import the `java.sql.Date` library to create a DataFrame with a `DateType` column.

```scala
import java.sql.Date
import org.apache.spark.sql.types.{DateType, IntegerType}

val sourceDF = spark.createDF(
  List(
    (1, Date.valueOf("2016-09-30")),
    (2, Date.valueOf("2016-12-14"))
  ), List(
    ("person_id", IntegerType, true),
    ("birth_date", DateType, true)
  )
)
```

```
sourceDF.show()

+---------+----------+
|person_id|birth_date|
+---------+----------+
|        1|2016-09-30|
|        2|2016-12-14|
+---------+----------+

sourceDF.printSchema()

root
 |-- person_id: integer (nullable = true)
 |-- birth_date: date (nullable = true)
```

The `cast()` method can create a `DateType` column by converting a `StringType` column into a date.

```scala
val sourceDF = spark.createDF(
  List(
    (1, "2013-01-30"),
    (2, "2012-01-01")
  ), List(
    ("person_id", IntegerType, true),
    ("birth_date", StringType, true)
  )
).withColumn(
  "birth_date",
  col("birth_date").cast("date")
)
```

```
sourceDF.show()

+---------+----------+
|person_id|birth_date|
+---------+----------+
|        1|2013-01-30|
|        2|2012-01-01|
+---------+----------+

sourceDF.printSchema()

root
 |-- person_id: integer (nullable = true)
 |-- birth_date: date (nullable = true)
```

## year(), month(), dayofmonth()

Let's create a DataFrame with a `DateType` column and use built in Spark functions to extract the year, month, and day from the date.

```scala
val sourceDF = spark.createDF(
  List(
    (1, Date.valueOf("2016-09-30")),
    (2, Date.valueOf("2016-12-14"))
  ), List(
    ("person_id", IntegerType, true),
    ("birth_date", DateType, true)
  )
)

sourceDF.withColumn(
  "birth_year",
  year(col("birth_date"))
).withColumn(
  "birth_month",
  month(col("birth_date"))
).withColumn(
  "birth_day",
  dayofmonth(col("birth_date"))
).show()
```

```
+---------+----------+----------+-----------+---------+
|person_id|birth_date|birth_year|birth_month|birth_day|
+---------+----------+----------+-----------+---------+
|        1|2016-09-30|      2016|          9|       30|
|        2|2016-12-14|      2016|         12|       14|
+---------+----------+----------+-----------+---------+
```

## minute(), second()

Let's create a DataFrame with a `TimestampType` column and use built in Spark functions to extract the minute and second from the timestamp.

```scala
import java.sql.Timestamp

val sourceDF = spark.createDF(
  List(
    (1, Timestamp.valueOf("2017-12-02 03:04:00")),
    (2, Timestamp.valueOf("1999-01-01 01:45:20"))
  ), List(
    ("person_id", IntegerType, true),
    ("fun_time", TimestampType, true)
  )
)

sourceDF.withColumn(
  "fun_minute",
  minute(col("fun_time"))
).withColumn(
  "fun_second",
  second(col("fun_time"))
).show()
```

```
+---------+-------------------+----------+----------+
|person_id|           fun_time|fun_minute|fun_second|
+---------+-------------------+----------+----------+
|        1|2017-12-02 03:04:00|         4|         0|
|        2|1999-01-01 01:45:20|        45|        20|
+---------+-------------------+----------+----------+
```

## datediff()

The `datediff()` and `current_date()` functions can be used to calculate the number of days between today and a date in a `DateType` column. Let's use these functions to calculate someone's age in days.

```scala
val sourceDF = spark.createDF(
  List(
    (1, Date.valueOf("1990-09-30")),
    (2, Date.valueOf("2001-12-14"))
  ), List(
    ("person_id", IntegerType, true),
    ("birth_date", DateType, true)
  )
)

sourceDF.withColumn(
  "age_in_days",
  datediff(current_timestamp(), col("birth_date"))
).show()
```

```
+---------+----------+-----------+
|person_id|birth_date|age_in_days|
+---------+----------+-----------+
|        1|1990-09-30|       9946|
|        2|2001-12-14|       5853|
+---------+----------+-----------+
```

## date\_add()

The `date_add()` function can be used to add days to a date. Let's add 15 days to a date column.

```scala
val sourceDF = spark.createDF(
  List(
    (1, Date.valueOf("1990-09-30")),
    (2, Date.valueOf("2001-12-14"))
  ), List(
    ("person_id", IntegerType, true),
    ("birth_date", DateType, true)
  )
)

sourceDF.withColumn(
  "15_days_old",
  date_add(col("birth_date"), 15)
).show()
```

```
+---------+----------+-----------+
|person_id|birth_date|15_days_old|
+---------+----------+-----------+
|        1|1990-09-30| 1990-10-15|
|        2|2001-12-14| 2001-12-29|
+---------+----------+-----------+
```

## Next steps

Look at the [Spark SQL functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) for the full list of methods available for working with dates and times in Spark.

The Spark date functions aren't comprehensive and Java / Scala datetime libraries are notoriously difficult to work with. We should think about filling in the gaps in the native Spark datetime libraries by adding functions to [spark-daria](https://github.com/MrPowers/spark-daria/).
