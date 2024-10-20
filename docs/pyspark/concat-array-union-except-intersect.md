---
title: "Combining PySpark arrays with concat, union, except and intersect"
date: "2021-05-01"
categories: 
  - "pyspark"
---

This post shows the different ways to combine multiple PySpark arrays into a single array.

These operations were difficult prior to Spark 2.4, but now there are built-in functions that make combining arrays easy.

## concat

`concat` joins two array columns into a single array.

Creating a DataFrame with two array columns so we can demonstrate with an example.

```
df = spark.createDataFrame(
    [(["a", "a", "b", "c"], ["c", "d"])], ["arr1", "arr2"]
)
df.show()
```

```
+------------+------+
|        arr1|  arr2|
+------------+------+
|[a, a, b, c]|[c, d]|
+------------+------+
```

Concatenate the two arrays with `concat`:

```
res = df.withColumn("arr_concat", concat(col("arr1"), col("arr2")))
res.show()
```

```
+------------+------+------------------+
|        arr1|  arr2|        arr_concat|
+------------+------+------------------+
|[a, a, b, c]|[c, d]|[a, a, b, c, c, d]|
+------------+------+------------------+
```

Notice that `arr_concat` contains duplicate values.

We can remove the duplicates with `array_distinct`:

```
df.withColumn(
    "arr_concat_distinct", array_distinct(concat(col("arr1"), col("arr2")))
).show()
```

```
+------------+------+-------------------+
|        arr1|  arr2|arr_concat_distinct|
+------------+------+-------------------+
|[a, a, b, c]|[c, d]|       [a, b, c, d]|
+------------+------+-------------------+
```

Let's look at another way to return a distinct concatenation of two arrays that isn't as verbose.

## array\_union

`array_union` combines two arrays, without any duplicates.

```
res = df.withColumn("arr_union", array_union(col("arr1"), col("arr2")))
res.show()
```

```
+------------+------+------------+
|        arr1|  arr2|   arr_union|
+------------+------+------------+
|[a, a, b, c]|[c, d]|[a, b, c, d]|
+------------+------+------------+
```

We can get the same result by nesting `concat` in `array_distinct`, but that's less efficient and unnecessarily verbose.

## array\_intersect

`array_intersect` returns the elements that are in both arrays.

```
res = df.withColumn("arr_intersect", array_intersect(col("arr1"), col("arr2")))
res.show()
```

```
+------------+------+-------------+
|        arr1|  arr2|arr_intersect|
+------------+------+-------------+
|[a, a, b, c]|[c, d]|          [c]|
+------------+------+-------------+
```

In our example, `c` is the only element that's in both `arr1` and `arr2`.

## array\_except

`array_except` returns a distinct list of the elements that are in `arr1`, but not in `arr2`.

```
res = df.withColumn("arr_except", array_except(col("arr1"), col("arr2")))
res.show()
```

```
+------------+------+----------+
|        arr1|  arr2|arr_except|
+------------+------+----------+
|[a, a, b, c]|[c, d]|    [a, b]|
+------------+------+----------+
```

`a` and `b` are in `arr1` and not in `arr2`.

## Conclusion

Several functions were added in PySpark 2.4 that make it significantly easier to work with array columns.

Earlier versions of Spark required you to write UDFs to perform basic array functions which was tedious.

This post doesn't cover all the important array functions. Make sure to also learn about the [exists and forall functions](https://mungingdata.com/pyspark/exists-forall-any-all-array/) and the transform / filter functions.

You'll be a PySpark array master once you're comfortable with these functions.
