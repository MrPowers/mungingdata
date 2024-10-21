---
title: "Exploring DataFrames with summary and describe"
date: "2021-04-16"
categories: 
  - "apache-spark"
---

# Exploring DataFrames with summary and describe

The `summary` and `describe` methods make it easy to explore the contents of a DataFrame at a high level.

This post shows you how to use these methods.

TL;DR - `summary` is more useful than `describe`. You can get the same result with `agg`, but `summary` will save you from writing a lot of code.

## describe

Suppose you have the following DataFrame.

```
+----+-------+
|num1|letters|
+----+-------+
|   1|     aa|
|   2|     aa|
|   9|     bb|
|   5|     cc|
+----+-------+
```

Use `describe` to compute some summary statistics on the DataFrame.

```
df.describe().show()
```

```
+-------+-----------------+-------+
|summary|             num1|letters|
+-------+-----------------+-------+
|  count|                4|      4|
|   mean|             4.25|   null|
| stddev|3.593976442141304|   null|
|    min|                1|     aa|
|    max|                9|     cc|
+-------+-----------------+-------+
```

You can limit the `describe` statistics for a subset of columns:

```
df.describe("num1").show()
```

```
+-------+-----------------+
|summary|             num1|
+-------+-----------------+
|  count|                4|
|   mean|             4.25|
| stddev|3.593976442141304|
|    min|                1|
|    max|                9|
+-------+-----------------+
```

This option isn't very useful. `df.select("num1").describe().show()` would give the same result and is more consistent with the rest of the Spark API.

Let's turn out attention to `summary`, a better designed method that provides more useful options.

## summary

Suppose you have the same starting DataFrame from before.

```
+----+-------+
|num1|letters|
+----+-------+
|   1|     aa|
|   2|     aa|
|   9|     bb|
|   5|     cc|
+----+-------+
```

Calculate the summary statistics for all columns in the DataFrame.

```
df.summary().show()
```

```
+-------+-----------------+-------+
|summary|             num1|letters|
+-------+-----------------+-------+
|  count|                4|      4|
|   mean|             4.25|   null|
| stddev|3.593976442141304|   null|
|    min|                1|     aa|
|    25%|                1|   null|
|    50%|                2|   null|
|    75%|                5|   null|
|    max|                9|     cc|
+-------+-----------------+-------+
```

Let's customize the output to return the count, 33rd percentile, 50th percentile, and 66th percentile.

```
df.summary("count", "33%", "50%", "66%").show()
```

```
+-------+----+-------+
|summary|num1|letters|
+-------+----+-------+
|  count|   4|      4|
|    33%|   2|   null|
|    50%|   2|   null|
|    66%|   5|   null|
+-------+----+-------+
```

Limit the custom summary to the `num1` column cause it doesn't make sense to compute percentiles for string columns.

```
df.select("num1").summary("count", "33%", "50%", "66%").show()
```

```
+-------+----+
|summary|num1|
+-------+----+
|  count|   4|
|    33%|   2|
|    50%|   2|
|    66%|   5|
+-------+----+
```

I worked with the Spark core team to add [some additional options](https://github.com/apache/spark/pull/31254) to this method, so as of Spark 3.2, you'll also be able to compute the exact and approximate count distinct.

Here's how to get the exact count and distinct count for each column:

```
df.summary("count", "count_distinct").show()
```

Here's how to get the approximate count distinct, which will run faster:

```
df.summary("count", "approx_count_distinct").show()
```

## agg

We can use `agg` to manually compute the summary statistics for columns in the DataFrame. Here's how to calculate the distinct count for each column in the DataFrame.

```
df.agg(countDistinct("num1"), countDistinct("letters")).show()
```

```
+-----------+--------------+
|count(num1)|count(letters)|
+-----------+--------------+
|          4|             3|
+-----------+--------------+
```

Here's how to calculate the distinct count and the max for each column in the DataFrame:

```
val counts = df.agg(
  lit("countDistinct").as("colName"),
  countDistinct("num1").as("num1"),
  countDistinct("letters").as("letters"))
val maxes = df.agg(
  lit("max").as("colName"),
  max("num1").as("num1"),
  max("letters").as("letters"))
counts.union(maxes).show()
```

```
+-------------+----+-------+
|      colName|num1|letters|
+-------------+----+-------+
|countDistinct|   4|      3|
|          max|   9|     cc|
+-------------+----+-------+
```

The code gets verbose quick. `summary` is great cause it prevents you from writing a lot of code.

## Conclusion

`summary` is great for high level exploratory data analysis.

For more detailed exploratory data analysis, see the [deequ](https://github.com/awslabs/deequ) library.

Ping me if you're interested and I'll add an extensible version of `summary` to [spark-daria](https://github.com/MrPowers/spark-daria). We should have a `dariaSummary` method that can be invoked like this `df.dariaSummary(countDistinct, max)` and automatically generate a custom summary, without forcing the user to write tons of code.
