---
title: "Renaming Multiple PySpark DataFrame columns (withColumnRenamed, select, toDF)"
date: "2020-07-19"
categories: 
  - "pyspark"
---

# Renaming Multiple PySpark DataFrame columns (withColumnRenamed, select, toDF)

This blog post explains how to rename one or all of the columns in a PySpark DataFrame.

You'll often want to rename columns in a DataFrame. Here are some examples:

- remove all spaces from the DataFrame columns
- convert all the columns to snake\_case
- [replace the dots in column names with underscores](https://mungingdata.com/pyspark/avoid-dots-periods-column-names/)

Lots of approaches to this problem are not scalable if you want to rename a lot of columns. Other solutions call `withColumnRenamed` a lot [which may cause performance issues](https://medium.com/@manuzhang/the-hidden-cost-of-spark-withcolumn-8ffea517c015) or cause [StackOverflowErrors](https://stackoverflow.com/questions/35066231/stack-overflow-while-processing-several-columns-with-a-udf).

This blog post outlines solutions that are easy to use and create simple analysis plans, so the Catalyst optimizer doesn't need to do hard optimization work.

## Renaming a single column using withColumnRenamed

Renaming a single column is easy with `withColumnRenamed`. Suppose you have the following DataFrame:

```
+----------+------------+
|first_name|likes_soccer|
+----------+------------+
|      jose|        true|
+----------+------------+
```

You can rename the `likes_soccer` column to `likes_football` with this code:

```
df.withColumnRenamed("likes_soccer", "likes_football").show()
```

```
+----------+--------------+
|first_name|likes_football|
+----------+--------------+
|      jose|          true|
+----------+--------------+
```

`withColumnRenamed` can also be used to rename all the columns in a DataFrame, but that's not a performant approach. Let's look at how to rename multiple columns in a performant manner.

## Renaming multiple columns

The [quinn](https://github.com/MrPowers/quinn/) library has a `with_columns_renamed` function that renames all the columns in a DataFrame.

Suppose you have the following DataFrame:

```
+-------------+-----------+
|i like cheese|yummy stuff|
+-------------+-----------+
|         jose|          a|
|           li|          b|
|          sam|          c|
+-------------+-----------+
```

Here's how to replace all the whitespace in the column names with underscores:

```
import quinn

def spaces_to_underscores(s):
    return s.replace(" ", "_")

actual_df = quinn.with_columns_renamed(spaces_to_underscores)(source_df)

actual_df.show()
```

```
+-------------+-----------+
|i_like_cheese|yummy_stuff|
+-------------+-----------+
|         jose|          a|
|           li|          b|
|          sam|          c|
+-------------+-----------+
```

This code generates an efficient parsed logical plan:

```
== Parsed Logical Plan ==
'Project ['`i.like.cheese` AS i_like_cheese#12, '`yummy.stuff` AS yummy_stuff#13]
+- LogicalRDD [i.like.cheese#8, yummy.stuff#9], false

== Analyzed Logical Plan ==
i_like_cheese: string, yummy_stuff: string
Project [i.like.cheese#8 AS i_like_cheese#12, yummy.stuff#9 AS yummy_stuff#13]
+- LogicalRDD [i.like.cheese#8, yummy.stuff#9], false

== Optimized Logical Plan ==
Project [i.like.cheese#8 AS i_like_cheese#12, yummy.stuff#9 AS yummy_stuff#13]
+- LogicalRDD [i.like.cheese#8, yummy.stuff#9], false

== Physical Plan ==
*(1) Project [i.like.cheese#8 AS i_like_cheese#12, yummy.stuff#9 AS yummy_stuff#13]
+- Scan ExistingRDD[i.like.cheese#8,yummy.stuff#9]
```

The parsed logical plan and the optimized logical plan are the same so the Spark Catalyst optimizer does not have to do any hard work.

`with_columns_renamed` takes two sets of arguments, so it can be [chained with the DataFrame transform method](https://mungingdata.com/pyspark/chaining-dataframe-transformations/). This code will give you the same result:

```
source_df.transform(quinn.with_columns_renamed(spaces_to_underscores))
```

The `transform` method is included in the PySpark 3 API. If you're using Spark 2, you need to monkey patch transform onto the DataFrame class, as described in this [the blog post](https://mungingdata.com/pyspark/chaining-dataframe-transformations/).

## with\_columns\_renamed source code

Here's the source code for the `with_columns_renamed` method:

```
def with_columns_renamed(fun):
    def _(df):
        cols = list(map(
            lambda col_name: F.col("`{0}`".format(col_name)).alias(fun(col_name)),
            df.columns
        ))
        return df.select(*cols)
    return _
```

The code creates a list of the new column names and runs a single select operation. As you've already seen, this code generates an efficient parsed logical plan.

Notice that this code does not run `withColumnRenamed` multiple times, like other implementations suggest. Calling `withColumnRenamed` many times [is a performance bottleneck](https://medium.com/@manuzhang/the-hidden-cost-of-spark-withcolumn-8ffea517c015).

## toDF for renaming columns

Let's use `toDF` to replace the whitespace with underscores (same objective, different implementation).

```
+-------------+-----------+
|i like cheese|yummy stuff|
+-------------+-----------+
|         jose|          a|
|           li|          b|
|          sam|          c|
+-------------+-----------+
```

Here's how we can update the column names with `toDF`:

```
df.toDF(*(c.replace(' ', '_') for c in df.columns)).show()
```

```
+-------------+-----------+
|i_like_cheese|yummy_stuff|
+-------------+-----------+
|         jose|          a|
|           li|          b|
|          sam|          c|
+-------------+-----------+
```

This approach generates an efficient parsed plan.

```
== Parsed Logical Plan ==
Project [i.like.cheese#0 AS i_like_cheese#11, yummy.stuff#1 AS yummy_stuff#12]
+- LogicalRDD [i.like.cheese#0, yummy.stuff#1], false

== Analyzed Logical Plan ==
i_like_cheese: string, yummy_stuff: string
Project [i.like.cheese#0 AS i_like_cheese#11, yummy.stuff#1 AS yummy_stuff#12]
+- LogicalRDD [i.like.cheese#0, yummy.stuff#1], false

== Optimized Logical Plan ==
Project [i.like.cheese#0 AS i_like_cheese#11, yummy.stuff#1 AS yummy_stuff#12]
+- LogicalRDD [i.like.cheese#0, yummy.stuff#1], false

== Physical Plan ==
*(1) Project [i.like.cheese#0 AS i_like_cheese#11, yummy.stuff#1 AS yummy_stuff#12]
```

You could also wrap this code in a function and give it a method signature so it can be chained with the `transform` method.

## withColumnRenamed antipattern when renaming multiple columns

You can call `withColumnRenamed` multiple times, but this isn't a good solution because it creates a complex parsed logical plan.

Here the `withColumnRenamed` implementation:

```
def rename_cols(df):
    for column in df.columns:
        new_column = column.replace('.','_')
        df = df.withColumnRenamed(column, new_column)
    return df

rename_cols(df).explain(True)
```

Here are the logical plans:

```
== Parsed Logical Plan ==
Project [i_like_cheese#31, yummy.stuff#28 AS yummy_stuff#34]
+- Project [i.like.cheese#27 AS i_like_cheese#31, yummy.stuff#28]
   +- LogicalRDD [i.like.cheese#27, yummy.stuff#28], false

== Analyzed Logical Plan ==
i_like_cheese: string, yummy_stuff: string
Project [i_like_cheese#31, yummy.stuff#28 AS yummy_stuff#34]
+- Project [i.like.cheese#27 AS i_like_cheese#31, yummy.stuff#28]
   +- LogicalRDD [i.like.cheese#27, yummy.stuff#28], false

== Optimized Logical Plan ==
Project [i.like.cheese#27 AS i_like_cheese#31, yummy.stuff#28 AS yummy_stuff#34]
+- LogicalRDD [i.like.cheese#27, yummy.stuff#28], false

== Physical Plan ==
*(1) Project [i.like.cheese#27 AS i_like_cheese#31, yummy.stuff#28 AS yummy_stuff#34]
```

The parsed and analyzed logical plans are more complex than what we've seen before. In this case, the Spark Catalyst optimizer is smart enough to perform optimizations and generate the same optimized logical plan, but parsing logical plans takes time [as described in this blog post](https://medium.com/@manuzhang/the-hidden-cost-of-spark-withcolumn-8ffea517c015). It's best to write code that's easy for Catalyst to optimize. When you can, write code that has simple parsed logical plans.

## Renaming some columns

Suppose you have the following DataFrame with column names that use British English. You'd like to convert these column names to American English (change chips to french\_fries and petrol to gas). You don't want to rename or remove columns that aren't being remapped to American English - you only want to change certain column names.

```
+------+-----+------+
| chips|   hi|petrol|
+------+-----+------+
|potato|hola!| disel|
+------+-----+------+
```

The [quinn](https://github.com/MrPowers/quinn/) `with_some_columns_renamed` function makes it easy to rename some columns.

```
import quinn

mapping = {"chips": "french_fries", "petrol": "gas"}

def british_to_american(s):
    return mapping[s]

def change_col_name(s):
    return s in mapping

actual_df = quinn.with_some_columns_renamed(british_to_american, change_col_name)(source_df)

actual_df.show()
```

```
+------------+-----+-----+
|french_fries|   hi|  gas|
+------------+-----+-----+
|      potato|hola!|disel|
+------------+-----+-----+
```

This code generates an efficient parsed logical plan:

```
== Parsed Logical Plan ==
'Project ['chips AS french_fries#32, unresolvedalias('hi, None), 'petrol AS gas#33]
+- LogicalRDD [chips#16, hi#17, petrol#18], false

== Analyzed Logical Plan ==
french_fries: string, hi: string, gas: string
Project [chips#16 AS french_fries#32, hi#17, petrol#18 AS gas#33]
+- LogicalRDD [chips#16, hi#17, petrol#18], false

== Optimized Logical Plan ==
Project [chips#16 AS french_fries#32, hi#17, petrol#18 AS gas#33]
+- LogicalRDD [chips#16, hi#17, petrol#18], false

== Physical Plan ==
*(1) Project [chips#16 AS french_fries#32, hi#17, petrol#18 AS gas#33]
```

The `with_some_columns_renamed` function takes two arguments:

- The first argument is a function specifies how the strings should be modified
- The second argument is a function that returns True if the string should be modified and False otherwise

## Replacing dots with underscores in column names

You should always replace dots with underscores in PySpark column names, [as explained in this post](https://mungingdata.com/pyspark/avoid-dots-periods-column-names/).

You can apply the methodologies you've learned in this blog post to easily replace dots with underscores.

## Next steps

It's important to write code that renames columns efficiently in Spark. You'll often start an analysis by read from a datasource and renaming the columns. If you use an inefficient renaming implementation, you're parsed logical plan will start out complex and will only get more complicated as you layer on more DataFrame transformations.

Complicated parsed logical plans are difficult for the Catalyst optimizer to optimize.

Make sure to read this blog post on [chaining DataFrame transformations](https://mungingdata.com/pyspark/chaining-dataframe-transformations/), so you're comfortable renaming columns with a function that's passed to the DataFrame#transform method. Writing elegant PySpark code will help you keep your notebooks clean and easy to read.
