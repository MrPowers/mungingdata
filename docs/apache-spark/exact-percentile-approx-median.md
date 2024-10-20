---
title: "Calculating Percentile, Approximate Percentile, and Median with Spark"
date: "2021-04-11"
categories: 
  - "apache-spark"
---

This blog post explains how to compute the percentile, approximate percentile and median of a column in Spark.

There are a variety of different ways to perform these computations and it's good to know all the approaches because they touch different important sections of the Spark API.

## Percentile

You can calculate the exact percentile with the `percentile` SQL function.

Suppose you have the following DataFrame:

```
+--------+
|some_int|
+--------+
|       0|
|      10|
+--------+
```

Calculate the 50th percentile:

```
df
  .agg(expr("percentile(some_int, 0.5)").as("50_percentile"))
  .show()
```

```
+-------------+
|50_percentile|
+-------------+
|          5.0|
+-------------+
```

Using `expr` to write SQL strings when using the Scala API isn't ideal. It's better to invoke Scala functions, but the `percentile` function isn't defined in the Scala API.

The [bebe](https://github.com/MrPowers/bebe) library fills in the Scala API gaps and provides easy access to functions like percentile.

```
df
  .agg(bebe_percentile(col("some_int"), lit(0.5)).as("50_percentile"))
  .show()
```

```
+-------------+
|50_percentile|
+-------------+
|          5.0|
+-------------+
```

`bebe_percentile` is implemented as a Catalyst expression, so it's just as performant as the SQL percentile function.

## Approximate Percentile

Create a DataFrame with the integers between 1 and 1,000.

```
val df1 = (1 to 1000).toDF("some_int")
```

Use the `approx_percentile` SQL method to calculate the 50th percentile:

```
df1
  .agg(expr("approx_percentile(some_int, array(0.5))").as("approx_50_percentile"))
  .show()
```

```
+--------------------+
|approx_50_percentile|
+--------------------+
|               [500]|
+--------------------+
```

This `expr` hack isn't ideal. We don't like including SQL strings in our Scala code.

Let's use the `bebe_approx_percentile` method instead.

```
df1
  .select(bebe_approx_percentile(col("some_int"), array(lit(0.5))).as("approx_50_percentile"))
  .show()
```

```
+--------------------+
|approx_50_percentile|
+--------------------+
|               [500]|
+--------------------+
```

bebe lets you write code that's a lot nicer and easier to reuse.

## Median

> The median is the value where fifty percent or the data values fall at or below it. Therefore, the median is the 50th percentile.

[Source](https://online.stat.psu.edu/stat500/book/export/html/536#:~:text=The%20median%20is%20the%20value,median%20is%20the%2050th%20percentile.)

We've already seen how to calculate the 50th percentile, or median, both exactly and approximately.

## Conclusion

The Spark percentile functions are exposed via the SQL API, but aren't exposed via the Scala or Python APIs.

Invoking the SQL functions with the expr hack is possible, but not desirable. Formatting large SQL strings in Scala code is annoying, especially when writing code that's sensitive to special characters (like a regular expression).

It's best to leverage the [bebe](https://github.com/MrPowers/bebe) library when looking for this functionality. The bebe functions are performant and provide a clean interface for the user.
