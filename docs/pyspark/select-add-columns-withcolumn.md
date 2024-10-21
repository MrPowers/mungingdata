---
title: "select and add columns in PySpark"
date: "2021-05-06"
categories: 
  - "pyspark"
---

# select and add columns in PySpark

This post shows you how to select a subset of the columns in a DataFrame with `select`. It also shows how `select` can be used to add and rename columns. Most PySpark users don't know how to truly harness the power of `select`.

This post also shows how to add a column with `withColumn`. Newbie PySpark developers often run `withColumn` multiple times to add multiple columns because there isn't a `withColumns` method. We will see why chaining multiple `withColumn` calls is an anti-pattern and how to avoid this pattern with `select`.

This post starts with basic use cases and then advances to the lesser-known, powerful applications of these methods.

## select basic use case

Create a DataFrame with three columns.

```
df = spark.createDataFrame(
    [("jose", 1, "mexico"), ("li", 2, "china"), ("sandy", 3, "usa")],
    ["name", "age", "country"],
)
df.show()
```

```
+-----+---+-------+
| name|age|country|
+-----+---+-------+
| jose|  1| mexico|
|   li|  2|  china|
|sandy|  3|    usa|
+-----+---+-------+
```

Select the `age` and `name` columns:

```
df.select("age", "name").show()
```

```
+---+-----+
|age| name|
+---+-----+
|  1| jose|
|  2|   li|
|  3|sandy|
+---+-----+
```

The `select` method takes column names as arguments.

If you try to select a column that doesn't exist in the DataFrame, your code will error out. Here's the error you'll see if you run `df.select("age", "name", "whatever")`.

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
           pyspark.sql.utils.AnalysisException: cannot resolve '`whatever`' given input columns: [age, country, name];
           'Project [age#77L, name#76, 'whatever]
           +- LogicalRDD [name#76, age#77L, country#78], false
```

Get used to parsing PySpark stack traces!

The `select` method can also take an array of column names as the argument.

```
df.select(["country", "name"]).show()
```

```
+-------+-----+
|country| name|
+-------+-----+
| mexico| jose|
|  china|   li|
|    usa|sandy|
+-------+-----+
```

You can also select based on an array of column objects:

```
df.select([col("age")]).show()
```

```
+---+
|age|
+---+
|  1|
|  2|
|  3|
+---+
```

Keep reading to see how selecting on an array of column object allows for advanced use cases, like renaming columns.

## withColumn basic use case

`withColumn` adds a column to a DataFrame.

Create a DataFrame with two columns:

```
df = spark.createDataFrame(
    [("jose", 1), ("li", 2), ("luisa", 3)], ["name", "age"]
)
df.show()
```

```
+-----+---+
| name|age|
+-----+---+
| jose|  1|
|   li|  2|
|luisa|  3|
+-----+---+
```

Append a `greeting` column to the DataFrame with the string `hello`:

```
df.withColumn("greeting", lit("hello")).show()
```

```
+-----+---+--------+
| name|age|greeting|
+-----+---+--------+
| jose|  1|   hello|
|   li|  2|   hello|
|luisa|  3|   hello|
+-----+---+--------+
```

Now let's use `withColumn` to append an `upper_name` column that uppercases the `name` column.

```
df.withColumn("upper_name", upper(col("name"))).show()
```

```
+-----+---+----------+
| name|age|upper_name|
+-----+---+----------+
| jose|  1|      JOSE|
|   li|  2|        LI|
|luisa|  3|     LUISA|
+-----+---+----------+
```

`withColumn` is often used to append columns based on the values of other columns.

## Add multiple columns (withColumns)

There isn't a `withColumns` method, so most PySpark newbies call `withColumn` multiple times when they need to add multiple columns to a DataFrame.

Create a simple DataFrame:

```
df = spark.createDataFrame(
    [("cali", "colombia"), ("london", "uk")],
    ["city", "country"],
)
df.show()
```

```
+------+--------+
|  city| country|
+------+--------+
|  cali|colombia|
|london|      uk|
+------+--------+
```

Here's how to append two columns with constant values to the DataFrame using `select`:

```
actual = df.select(["*", lit("val1").alias("col1"), lit("val2").alias("col2")])
actual.show()
```

```
+------+--------+----+----+
|  city| country|col1|col2|
+------+--------+----+----+
|  cali|colombia|val1|val2|
|london|      uk|val1|val2|
+------+--------+----+----+
```

The `*` selects all of the existing DataFrame columns and the other columns are appended. This design pattern is how `select` can append columns to a DataFrame, just like `withColumn`.

The code is a bit verbose, but it's better than the following code that calls `withColumn` multiple times:

```
df.withColumn("col1", lit("val1")).withColumn("col2", lit("val2"))
```

There is a [hidden cost of withColumn](https://medium.com/@manuzhang/the-hidden-cost-of-spark-withcolumn-8ffea517c015) and calling it multiple times should be avoided.

The Spark contributors are [considering adding withColumns to the API](https://github.com/apache/spark/pull/32431), which would be the best option. That'd give the community a clean and performant way to add multiple columns.

## Snake case all columns

Create a DataFrame with annoyingly named columns:

```
annoying = spark.createDataFrame(
    [(3, "mystery"), (23, "happy")],
    ["COOL NUMBER", "RELATED EMOTION"],
)
annoying.show()
```

```
+-----------+---------------+
|COOL NUMBER|RELATED EMOTION|
+-----------+---------------+
|          3|        mystery|
|         23|          happy|
+-----------+---------------+
```

Gross.

Write some code that'll convert all the column names to snake\_case:

```
def to_snake_case(s):
    return s.lower().replace(" ", "_")

cols = [col(s).alias(to_snake_case(s)) for s in annoying.columns]
annoying.select(cols).show()
```

```
+-----------+---------------+
|cool_number|related_emotion|
+-----------+---------------+
|          3|        mystery|
|         23|          happy|
+-----------+---------------+
```

Some DataFrames have hundreds or thousands of columns, so it's important to know how to rename all the columns programatically with a loop, followed by a `select`.

## Remove dots from all column names

Create a DataFrame with dots in the column names:

```
annoying = spark.createDataFrame(
    [(3, "mystery"), (23, "happy")],
    ["cool.number", "related.emotion"],
)
annoying.show()
```

```
+-----------+---------------+
|cool.number|related.emotion|
+-----------+---------------+
|          3|        mystery|
|         23|          happy|
+-----------+---------------+
```

Remove the dots from the column names and replace them with underscores.

```
cols = [col("`" + s + "`").alias(s.replace(".", "_")) for s in annoying.columns]
annoying.select(cols).show()
```

```
+-----------+---------------+
|cool_number|related_emotion|
+-----------+---------------+
|          3|        mystery|
|         23|          happy|
+-----------+---------------+
```

Notice that this code hacks in backticks around the column name or else it'll error out (simply calling `col(s)` will cause an error in this case). These backticks are needed whenever the column name contains periods. Super annoying.

You should never have dots in your column names [as discussed in this post](https://mungingdata.com/pyspark/avoid-dots-periods-column-names/). Dots in column names cause weird bugs. Always get rid of dots in column names whenever you see them.

## Conclusion

The `select` method can be used to grab a subset of columns, rename columns, or append columns. It's a powerful method that has a variety of applications.

`withColumn` is useful for adding a single column. It shouldn't be chained when adding multiple columns (fine to chain a few times, but shouldn't be chained hundreds of times). You now know how to append multiple columns with `select`, so you can avoid chaining `withColumn` calls.

Hopefully `withColumns` is added to the PySpark codebase so it's even easier to add multiple columns.
