---
title: "Calculating Month Start and End Dates with Spark"
date: "2021-01-02"
categories: 
  - "apache-spark"
---

# Calculating Month Start and End Dates with Spark

This post shows how to create `beginningOfMonthDate` and `endOfMonthDate` functions by leveraging the native Spark datetime functions.

The native Spark datetime functions are not easy to use, so it's important to build abstractions on top of the standard lib. Using the standard lib functions directly results in code that's difficult to understand.

## Month end

Spark has a function that calculates the last day of the month, but it's poorly named. Let's give the Spark function a more descriptive name so our code is readable.

```scala
def endOfMonthDate(col: Column): Column = {
  last_day(col)
}
```

You can access this function via the [spark-daria](https://github.com/MrPowers/spark-daria) library if you don't want to define it yourself.

Suppose you have the following data:

```
+----------+
| some_date|
+----------+
|2016-09-10|
|2020-01-01|
|2016-01-10|
|      null|
+----------+
```

Append an `end_of_month` column to the DataFrame:

```scala
import com.github.mrpowers.spark.daria.sql.functions._

df
  .withColumn("end_of_month", endOfMonthDate(col("some_date")))
  .show()
```

Observe the results:

```
+----------+------------+
| some_date|end_of_month|
+----------+------------+
|2016-09-10|  2016-09-30|
|2020-01-01|  2020-01-31|
|2016-01-10|  2016-01-31|
|      null|        null|
+----------+------------+
```

## Month start

You can calculate the start of the month with the `trunc` or `date_trunc` functions. Suppose you have the following DataFrame with a date column:

```
+----------+
| some_date|
+----------+
|2017-11-25|
|2017-12-21|
|2017-09-12|
|      null|
+----------+
```

Here are the two different ways to calculate the beginning of the month:

```scala
datesDF
  .withColumn("beginning_of_month_date", trunc(col("some_date"), "month"))
  .withColumn("beginning_of_month_time", date_trunc("month" ,col("some_date")))
  .show()
```

```
+----------+-----------------------+-----------------------+
| some_date|beginning_of_month_date|beginning_of_month_time|
+----------+-----------------------+-----------------------+
|2017-11-25|             2017-11-01|    2017-11-01 00:00:00|
|2017-12-21|             2017-12-01|    2017-12-01 00:00:00|
|2017-09-12|             2017-09-01|    2017-09-01 00:00:00|
|      null|                   null|                   null|
+----------+-----------------------+-----------------------+
```

Important observations:

- `trunc` returns a date column and `date_trunc` returns a timestamp column
- `trunc` takes `col("some_date")` as the first argument and `date_trunc` takes `col("some_date")` as the second argument. They're inconsistent.
- `date_trunc` sounds like it should be returning a date column. It's not named well.

Let's define `beginningOfMonthDate` and `beginningOfMonthTime` functions that are more intuitive.

```scala
def beginningOfMonthDate(col: Column): Column = {
  trunc(col, "month")
}

def beginningOfMonthTime(col: Column): Column = {
  date_trunc("month", col)
}
```

These functions let us write code that's easier to read:

```scala
datesDF
  .withColumn("beginning_of_month_date", beginningOfMonthDate(col("some_date")))
  .withColumn("beginning_of_month_time", beginningOfMonthTime(col("some_date")))
  .show()
```

These functions are defined in [spark-daria](https://github.com/MrPowers/spark-daria).

## Next steps

Spark's standard datetime functions aren't the best, but they're still better than building UDFs with the `java.time` library.

Using the [spark-daria](https://github.com/MrPowers/spark-daria) datetime abstractions is the best way to create readable code.

The spark-daria datetime function names are based on Rails, which is a well designed datetime library.

See [this post](https://mungingdata.com/apache-spark/week-end-start-dayofweek-next-day/) for a detailed explanation on how spark-daria makes computing the week start / week end / next weekday easy. These are examples of core datetime functionality that should be abstracted in an open source library. You shouldn't need to reinvent the wheel and write core datetime logic in your applications.
