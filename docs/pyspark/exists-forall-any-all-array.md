---
title: "exists and forall PySpark array functions"
date: "2021-05-01"
categories: 
  - "pyspark"
---

This blog post demonstrates how to find if any element in a PySpark array meets a condition with `exists` or if all elements in an array meet a condition with `forall`.

`exists` is similar to the Python `any` function. `forall` is similar to the Python `all` function.

## exists

This section demonstrates how `any` is used to determine if one or more elements in an array meets a certain predicate condition and then shows how the PySpark `exists` method behaves in a similar manner.

Create a regular Python array and use `any` to see if it contains the letter `b`.

```
arr = ["a", "b", "c"]
any(e == "b" for e in arr) # True
```

We can also wrap `any` in a function that's takes array and anonymous function arguments. This is similar to what we'll see in PySpark.

```
def any_lambda(iterable, function):
  return any(function(i) for i in iterable)

equals_b = lambda e: e == "b"

any_lambda(arr, equals_b) # True
```

We've seen how `any` works with vanilla Python. Let's see how `exists` works similarly with a PySpark array column.

Create a DataFrame with an array column.

```
df = spark.createDataFrame(
    [(["a", "b", "c"],), (["x", "y", "z"],)], ["some_arr"]
)
df.show()
```

```
+---------+
| some_arr|
+---------+
|[a, b, c]|
|[x, y, z]|
+---------+
```

Append a column that returns `True` if the array contains the letter `b` and `False` otherwise.

```
equals_b = lambda e: e == "b"
res = df.withColumn("has_b", exists(col("some_arr"), equals_b))
res.show()
```

```
+---------+-----+
| some_arr|has_b|
+---------+-----+
|[a, b, c]| true|
|[x, y, z]|false|
+---------+-----+
```

The `exists` function takes an array column as the first argument and an anonymous function as the second argument.

## forall

`all` is used to determine if every element in an array meets a certain predicate condition.

Create an array of numbers and use `all` to see if every number is even.

```
nums = [1, 2, 3]
all(e % 2 == 0 for e in nums) # False
```

You can also wrap `all` in a function that's easily invoked with an array and an anonymous function.

```
def all_lambda(iterable, function):
  return all(function(i) for i in iterable)

is_even = lambda e: e % 2 == 0

evens = [2, 4, 8]
all_lambda(evens, is_even) # True
```

`forall` in PySpark behaves like `all` in vanilla Python.

Create a DataFrame with an array column.

```
df = spark.createDataFrame(
    [([1, 2, 3],), ([2, 6, 12],)], ["some_arr"]
)
df.show()
```

```
+----------+
|  some_arr|
+----------+
| [1, 2, 3]|
|[2, 6, 12]|
+----------+
```

Append a column that returns `True` if the array only contains even numbers and `False` otherwise.

```
is_even = lambda e: e % 2 == 0
res = df.withColumn("all_even", forall(col("some_arr"), is_even))
res.show()
```

```
+----------+--------+
|  some_arr|all_even|
+----------+--------+
| [1, 2, 3]|   false|
|[2, 6, 12]|    true|
+----------+--------+
```

## Conclusion

`exists` and `forall` are flexible because they're invoked with a function argument. These functions are easily adaptable for lots of use cases.

You'll often work with array columns and these functions will easily allow you to code complex logic.
