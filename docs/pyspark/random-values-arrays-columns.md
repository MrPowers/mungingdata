---
title: "Fetching Random Values from PySpark Arrays / Columns"
date: "2020-07-26"
categories: 
  - "pyspark"
---

This post shows you how to fetch a random value from a PySpark array or from a set of columns. It'll also show you how to add a column to a DataFrame with a random value from a Python array and how to fetch n random values from a given column.

## Random value from PySpark array

Suppose you have the following DataFrame:

```
+------------+
|     letters|
+------------+
|   [a, b, c]|
|[a, b, c, d]|
|         [x]|
|          []|
+------------+
```

You can leverage the `array_choice()` function defined in [quinn](https://github.com/MrPowers/quinn/) to append a `random_letter` column that fetches a random value from `letters`.

```
actual_df = df.withColumn(
    "random_letter",
    quinn.array_choice(F.col("letters"))
)
actual_df.show()
```

```
+------------+-------------+
|     letters|random_letter|
+------------+-------------+
|   [a, b, c]|            c|
|[a, b, c, d]|            c|
|         [x]|            x|
|          []|         null|
+------------+-------------+
```

Here's how the `array_choice()` function is defined:

```
import pyspark.sql.functions as F

def array_choice(col):
    index = (F.rand()*F.size(col)).cast("int")
    return col[index]
```

## Random value from columns

You can also use `array_choice` to fetch a random value from a list of columns. Suppose you have the following DataFrame:

```
+----+----+----+
|num1|num2|num3|
+----+----+----+
|   1|   2|   3|
|   4|   5|   6|
|   7|   8|   9|
|  10|null|null|
|null|null|null|
+----+----+----+
```

Here's the code to append a `random_number` column that selects a random value from `num1`, `num2`, or `num3`.

```
actual_df = df.withColumn(
    "random_number",
    quinn.array_choice(F.array(F.col("num1"), F.col("num2"), F.col("num3")))
)
actual_df.show()
```

```
+----+----+----+-------------+
|num1|num2|num3|random_number|
+----+----+----+-------------+
|   1|   2|   3|            1|
|   4|   5|   6|            4|
|   7|   8|   9|            8|
|  10|null|null|           10|
|null|null|null|         null|
+----+----+----+-------------+
```

The `array` function is used to convert the columns to an array, so the input is suitable for `array_choice`.

## Random value from Python array

Suppose you'd like to add a `random_animal` column to an existing DataFrame that randomly selects between cat, dog, and mouse.

```
df = spark.createDataFrame([('jose',), ('maria',), (None,)], ['first_name'])
cols = list(map(lambda col_name: F.lit(col_name), ['cat', 'dog', 'mouse']))
actual_df = df.withColumn(
    'random_animal',
    quinn.array_choice(F.array(*cols))
)
actual_df.show()
```

```
+----------+-------------+
|first_name|random_animal|
+----------+-------------+
|      jose|          cat|
|     maria|        mouse|
|      null|          dog|
+----------+-------------+
```

This tactic is useful when you're creating fake datasets.

Study this code closely and make sure you're comfortable with making a list of PySpark column objects (this line of code: `cols = list(map(lambda col_name: F.lit(col_name), ['cat', 'dog', 'mouse']))`). Manipulating lists of PySpark columns is useful when [renaming multiple columns](https://mungingdata.com/pyspark/rename-multiple-columns-todf-withcolumnrenamed/), when [removing dots from column names](https://mungingdata.com/pyspark/avoid-dots-periods-column-names/) and when changing column types. It's an important design pattern for PySpark programmers to master.

## N random values from a column

Suppose you'd like to get some random values from a PySpark column, [as discussed here](https://stackoverflow.com/questions/46564657/pyspark-how-to-get-random-values-from-a-dataframe-column). Here's a sample DataFrame:

```
+---+
| id|
+---+
|123|
|245|
| 12|
|234|
+---+
```

Here's how to fetch three random values from the `id` column:

```
df.rdd.takeSample(False, 3)
```

Here's how you get the result as an array of integers:

```
list(map(lambda row: row[0], df.rdd.takeSample(False, 3))) # => [123, 12, 245]
```

This code also works, but requires a full table sort which is expensive:

```
df.select('id').orderBy(F.rand()).limit(3)
```

Examine the physical plan to verify that a full table sort is performed:

```
df.select('id').orderBy(F.rand()).limit(3).explain()
```

Here's the physical plan that's outputted:

```
TakeOrderedAndProject(limit=3, orderBy=[_nondeterministic#38 ASC NULLS FIRST], output=[id#32L])
+- *(1) Project [id#32L, rand(-4436287143488772163) AS _nondeterministic#38]
```

If your table is huge, then a full table sort will be slow.

## Next steps

Feel free to copy / paste `array_choice` in your notebooks or depend on [quinn](https://github.com/MrPowers/quinn/) to access this functionality.

Notebooks that don't rely on open source / private code abstractions tend to be overly complex. Think about moving generic code like `array_choice` to codebases outside your notebook. Solving problems with PySpark is hard enough. Don't make it harder by bogging down your notebooks with additional complexity.

Read the blog posts on [creating a PySpark project with Poetry](https://mungingdata.com/pyspark/poetry-dependency-management-wheel/) and [testing PySpark code](https://mungingdata.com/pyspark/testing-pytest-chispa/) to learn more about PySpark best practices.
