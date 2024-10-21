---
title: "Filtering PySpark Arrays and DataFrame Array Columns"
date: "2021-05-04"
categories: 
  - "pyspark"
---

# Filtering PySpark Arrays and DataFrame Array Columns

This post explains how to filter values from a PySpark array column.

It also explains how to filter DataFrames with array columns (i.e. reduce the number of rows in a DataFrame).

Filtering values from an ArrayType column and filtering DataFrame rows are completely different operations of course. The `pyspark.sql.DataFrame#filter` method and the `pyspark.sql.functions#filter` function share the same name, but have different functionality. One removes elements from an array and the other removes rows from a DataFrame.

It's important to understand both. The rest of this post provides clear examples.

## filter array column

Suppose you have the following DataFrame with a `some_arr` column that contains numbers.

```
df = spark.createDataFrame(
    [([1, 2, 3, 5, 7],), ([2, 4, 9],)], ["some_arr"]
)
df.show()
```

```
+---------------+
|       some_arr|
+---------------+
|[1, 2, 3, 5, 7]|
|      [2, 4, 9]|
+---------------+
```

Use `filter` to append an `arr_evens` column that only contains the even numbers from `some_arr`:

```
from pyspark.sql.functions import *

is_even = lambda x: x % 2 == 0
res = df.withColumn("arr_evens", filter(col("some_arr"), is_even))
res.show()
```

```
+---------------+---------+
|       some_arr|arr_evens|
+---------------+---------+
|[1, 2, 3, 5, 7]|      [2]|
|      [2, 4, 9]|   [2, 4]|
+---------------+---------+
```

The vanilla `filter` method in Python works similarly:

```
list(filter(is_even, [2, 4, 9])) # [2, 4]
```

The Spark filter function takes `is_even` as the second argument and the Python filter function takes `is_even` as the first argument. It's never easy ;)

Now let's turn our attention to filtering entire rows.

## filter rows if array column contains a value

Suppose you have the following DataFrame.

```
df = spark.createDataFrame(
    [(["one", "two", "three"],), (["four", "five"],), (["one", "nine"],)], ["some_arr"]
)
df.show()
```

```
+-----------------+
|         some_arr|
+-----------------+
|[one, two, three]|
|     [four, five]|
|      [one, nine]|
+-----------------+
```

Here's how to filter out all the rows that don't contain the string `one`:

```
res = df.filter(array_contains(col("some_arr"), "one"))
res.show()
```

```
+-----------------+
|         some_arr|
+-----------------+
|[one, two, three]|
|      [one, nine]|
+-----------------+
```

`array_contains` makes for clean code.

`where()` is an alias for filter so `df.where(array_contains(col("some_arr"), "one"))` will return the same result.

## filter on if at least one element in an array meets a condition

Create a DataFrame with some words:

```
df = spark.createDataFrame(
    [(["apple", "pear"],), (["plan", "pipe"],), (["cat", "ant"],)], ["some_words"]
)
df.show()
```

```
+-------------+
|   some_words|
+-------------+
|[apple, pear]|
| [plan, pipe]|
|   [cat, ant]|
+-------------+
```

Filter out all the rows that don't contain a word that starts with the letter `a`.

```
starts_with_a = lambda s: s.startswith("a")
res = df.filter(exists(col("some_words"), starts_with_a))
res.show()
```

```
+-------------+
|   some_words|
+-------------+
|[apple, pear]|
|   [cat, ant]|
+-------------+
```

`exists` lets you model powerful filtering logic.

See the [PySpark exists and forall](https://mungingdata.com/pyspark/exists-forall-any-all-array/) post for a detailed discussion of `exists` and the other method we'll talk about next, `forall`.

## filter if all elements in an array meet a condition

Create a DataFrame with some integers:

```
df = spark.createDataFrame(
    [([1, 2, 3, 5, 7],), ([2, 4, 9],), ([2, 4, 6],)], ["some_ints"]
)
df.show()
```

```
+---------------+
|      some_ints|
+---------------+
|[1, 2, 3, 5, 7]|
|      [2, 4, 9]|
|      [2, 4, 6]|
+---------------+
```

Filter out all the rows that contain any odd numbers.

```
is_even = lambda x: x % 2 == 0
res = df.filter(forall(col("some_ints"), is_even))
res.show()
```

```
+---------+
|some_ints|
+---------+
|[2, 4, 6]|
+---------+
```

`forall` is useful when filtering.

## Conclusion

PySpark has a `pyspark.sql.DataFrame#filter` method and a separate `pyspark.sql.functions.filter` function. Both are important, but they're useful in completely different contexts.

The `filter` function was added in Spark 3.1, whereas the `filter` method has been around since the early days of Spark (1.3). The filter method is especially powerful when used with multiple conditions or with `forall` / `exsists` (methods added in Spark 3.1).

PySpark 3 has added a lot of developer friendly functions and makes big data processing with Python a delight.
