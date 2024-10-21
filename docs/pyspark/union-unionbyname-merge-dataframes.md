---
title: "Combining PySpark DataFrames with union and unionByName"
date: "2021-05-04"
categories: 
  - "pyspark"
---

# Combining PySpark DataFrames with union and unionByName

Multiple PySpark DataFrames can be combined into a single DataFrame with `union` and `unionByName`.

`union` works when the columns of both DataFrames being joined are in the same order. It can give surprisingly wrong results when the schemas aren't the same, so watch out!

`unionByName` works when both DataFrames have the same columns, but in a different order. An optional parameter was also added in Spark 3.1 to allow unioning slightly different schemas.

This post explains how to use both methods and gives details on how the operations function under the hood.

## union

Suppose you have the following `americans` DataFrame:

```
+----------+---+
|first_name|age|
+----------+---+
|       bob| 42|
|      lisa| 59|
+----------+---+
```

And the following `colombians` DataFrame:

```
+----------+---+
|first_name|age|
+----------+---+
|     maria| 20|
|    camilo| 31|
+----------+---+
```

Here's how to union the two DataFrames.

```
res = americans.union(colombians)
res.show()
```

```
+----------+---+
|first_name|age|
+----------+---+
|       bob| 42|
|      lisa| 59|
|     maria| 20|
|    camilo| 31|
+----------+---+
```

Here's the full code snippet in case you'd like to run this code on your local machine.

```
americans = spark.createDataFrame(
    [("bob", 42), ("lisa", 59)], ["first_name", "age"]
)
colombians = spark.createDataFrame(
    [("maria", 20), ("camilo", 31)], ["first_name", "age"]
)
res = americans.union(colombians)
res.show()
```

Suppose you have a `brasilians` DataFrame with `age` and `first_name` columns - the same columns as before but in reverse order.

```
+---+----------+
|age|first_name|
+---+----------+
| 33|     tiago|
| 36|     lilly|
+---+----------+
```

If we union `americans` and `brasilians` with `americans.union(brasilans)`, we will get an incorrect result.

```
+----------+-----+
|first_name|  age|
+----------+-----+
|       bob|   42|
|      lisa|   59|
|        33|tiago|
|        36|lilly|
+----------+-----+
```

Oh my, this is really bad.

Here's `americans.printSchema()`:

```
root
 |-- first_name: string (nullable = true)
 |-- age: long (nullable = true)
```

Here's `brasilians.printSchema()`:

```
root
 |-- age: long (nullable = true)
 |-- first_name: string (nullable = true)
```

PySpark is unioning different types - that's definitely not what you want.

Let's look at a solution that gives the correct result when the columns are in a different order.

## unionByName

`unionByName` joins by column names, not by the order of the columns, so it can properly combine two DataFrames with columns in different orders.

Let's try combining `americans` and `brasilians` with `unionByName`.

```
res = americans.unionByName(brasilans)
res.show()
```

```
+----------+---+
|first_name|age|
+----------+---+
|       bob| 42|
|      lisa| 59|
|     tiago| 33|
|     lilly| 36|
+----------+---+
```

`unionByName` gives a correct result here, unlike the wrong answer we got with `union`.

Let's create an `indians` DataFrame with `age`, `first_name`, and `hobby` columns:

```
indians = spark.createDataFrame(
    [(55, "arjun", "cricket"), (5, "ira", "playing")], ["age", "first_name", "hobby"]
)
indians.show()
```

```
+---+----------+-------+
|age|first_name|  hobby|
+---+----------+-------+
| 55|     arjun|cricket|
|  5|       ira|playing|
+---+----------+-------+
```

Now union `americans` with `indians`:

```
res = americans.unionByName(indians)
res.show()
```

This'll error out with the following message.

```
def deco(*a, **kw):
    try:
        return f(*a, **kw)
    except py4j.protocol.Py4JJavaError as e:
        converted = convert_exception(e.java_exception)
        if not isinstance(converted, UnknownException):
            # Hide where the exception came from that shows a non-Pythonic
            # JVM exception message.
           raise converted from None
           pyspark.sql.utils.AnalysisException: Union can only be performed on tables with the same number of columns, but the first table has 2 columns and the second table has 3 columns;
           'Union false, false
           :- LogicalRDD [first_name#219, age#220L], false
           +- Project [first_name#224, age#223L, hobby#225]
              +- LogicalRDD [age#223L, first_name#224, hobby#225], false
```

In PySpark 3.1.0, an optional `allowMissingColumns` argument was added, which allows DataFrames with different schemas to be unioned.

```
americans.unionByName(indians, allowMissingColumns = True).show()
```

```
+----------+---+-------+
|first_name|age|  hobby|
+----------+---+-------+
|       bob| 42|   null|
|      lisa| 59|   null|
|     arjun| 55|cricket|
|       ira|  5|playing|
+----------+---+-------+
```

Here's the ugly code you needed to write before `allowMissingColumns` was added:

```
for c in missing_col_names: americans = americans.withColumn(c, lit(None))
res = americans.union(indians.select("first_name", "age", "hobby"))
res.show()
```

```
+----------+---+-------+
|first_name|age|  hobby|
+----------+---+-------+
|       bob| 42|   null|
|      lisa| 59|   null|
|     arjun| 55|cricket|
|       ira|  5|playing|
+----------+---+-------+
```

The `allowMissingColumns` option lets you focus on your application logic instead of getting caught up with the Python code.

Thanks to [Vegetable\_Hamster732](https://www.reddit.com/r/apachespark/comments/n4c83m/allowmissingcolumns_option_added_to_unionbyname/gwx4l4q?utm_source=share&utm_medium=web2x&context=3) for sharing this solution.

## unionAll

`unionAll` is an alias for `union` and should be avoided.

`unionAll` was used in older versions of PySpark and now `union` is preferred.

## Conclusion

`union` is generally sufficient, but use `unionByName` if you want to be safe and avoid the weird bug we pointed out in this post.

The `allowMissingColumns` argument makes `unionByName` easier to use when the schemas don't line up exactly. In earlier versions of PySpark, it was annoying to manually add `null` columns before running `union` to account for DataFrames with slightly different schemas.

The PySpark maintainers are doing a great job incrementally improving the API to make it more developer friendly. The changes are backwards compatible, so we get new features without breaking changes. Great job Spark core team!
