---
title: "Chaining Custom PySpark DataFrame Transformations"
date: "2017-10-31"
categories: 
  - "pyspark"
---

# Chaining Custom PySpark DataFrame Transformations

PySpark code should generally be organized as single purpose DataFrame transformations that can be chained together for production analyses (e.g. generating a datamart).

This blog post demonstrates how to monkey patch the DataFrame object with a transform method, how to define custom DataFrame transformations, and how to chain the function calls.

We'll also demonstrate how to run multiple custom transformations with function composition using the [cytoolz](https://github.com/pytoolz/cytoolz) library.

If you're using the Scala API, [read this blog post](https://mungingdata.com/apache-spark/chaining-custom-dataframe-transformations/) on chaining DataFrame transformations with Scala.

## Accessing DataFrame transform method

Spark 3 [includes a native DataFrame transform method](https://github.com/apache/spark/pull/23877), so Spark 3 users can skip the rest of this section.

Spark 2 users can monkey patch the DataFrame object with a transform method so we can chain DataFrame transformations.

```
from pyspark.sql.dataframe import DataFrame


def transform(self, f):
    return f(self)


DataFrame.transform = transform
```

This code snippet is from the [quinn project](https://github.com/MrPowers/quinn/).

## Chaining DataFrame Transformations with lambda

Let's define a couple of simple DataFrame transformations to test the transform method.

```
def with_greeting(df):
    return df.withColumn("greeting", lit("hi"))

def with_something(df, something):
    return df.withColumn("something", lit(something))
```

Let's create a DataFrame and then run the `with_greeting` and `with_something` DataFrame transformations.

```
data = [("jose", 1), ("li", 2), ("liz", 3)]
source_df = spark.createDataFrame(data, ["name", "age"])

actual_df = (source_df
    .transform(lambda df: with_greeting(df))
    .transform(lambda df: with_something(df, "crazy")))
```

```
print(actual_df.show())

+----+---+--------+---------+
|name|age|greeting|something|
+----+---+--------+---------+
|jose|  1|      hi|    crazy|
|  li|  2|      hi|    crazy|
| liz|  3|      hi|    crazy|
+----+---+--------+---------+
```

The `lambda` is optional for custom DataFrame transformations that only take a single DataFrame argument so we can refactor `with_greeting` line as follows:

```
actual_df = (source_df
    .transform(with_greeting)
    .transform(lambda df: with_something(df, "crazy")))
```

Without the `DataFrame#transform` method, we would have needed to write code like this:

```
df1 = with_greeting(source_df)
actual_df = with_something(df1, "moo")
```

The transform method improves our code by helping us avoid multiple order dependent variable assignments. Creating multiple variables gets especially ugly when 5+ transformations need to be run. You don't want df1, df2, df3, df4, and df5 😡

Let's define a DataFrame transformation with an alternative method signature to allow for easier chaining 😅

## Chaining DataFrame Transformations with functools.partial

Let's define a `with_jacket` DataFrame transformation that appends a `jacket` column to a DataFrame.

```
def with_jacket(word, df):
    return df.withColumn("jacket", lit(word))
```

We'll use the same source\_df DataFrame and with\_greeting method from before and chain the transformations with `functools.partial`.

```
from functools import partial

actual_df = (source_df
    .transform(with_greeting)
    .transform(partial(with_jacket, "warm")))
```

```
print(actual_df.show())

+----+---+--------+------+
|name|age|greeting|jacket|
+----+---+--------+------+
|jose|  1|      hi|  warm|
|  li|  2|      hi|  warm|
| liz|  3|      hi|  warm|
+----+---+--------+------+
```

`functools.partial` helps us get rid of the `lambda` functions, but we can do even better…

## Defining DataFrame transformations as nested functions

DataFrame transformations that are defined with nested functions have the most elegant interface for chaining. Let's define a `with_funny` function that appends a `funny` column to a DataFrame.

```
def with_funny(something_funny):
    return lambda df: (
        df.withColumn("funny1", F.lit(something_funny))
    )
```

We'll use the same `source_df` DataFrame and `with_greeting` method from before.

```
actual_df = (source_df
     .transform(with_greeting)
     .transform(with_funny("haha")))
```

```
print(actual_df.show())

+----+---+--------+-----+
|name|age|greeting|funny|
+----+---+--------+-----+
|jose|  1|      hi| haha|
|  li|  2|      hi| haha|
| liz|  3|      hi| haha|
+----+---+--------+-----+
```

This is much better! 🎊. Thanks for suggesting this implementation [hoffrocket](https://github.com/hoffrocket)!

We can also define a custom transformation with an inner function (the inner function underscore in this example).

```
def with_funny(word):
    def _(df):
        return df.withColumn("funny", lit(word))
    return _
```

The inner function is named `_`. If you're going to explicitly name the inner function, using an underscore is a good choice because it's easy to apply consistently throughout the codebase. This design pattern was suggested by the developer that added the `transform` method to the DataFrame API, [see here](https://github.com/apache/spark/pull/23877#issuecomment-649198577).

```
def with_funny(word):
    def _(df):
        return df.withColumn("funny", lit(word))
    return _
```

The inner function is named `_`. Naming the inner function as underscore makes it easier to build a consistent codebase, as suggested by the developer that added the `transform` method to the DataFrame API, [see here](https://github.com/apache/spark/pull/23877#issuecomment-649198577).

## Function composition with cytoolz

We can define custom DataFrame transformations with the `@curry` decorator and run them with function composition provided by `cytoolz`.

```
from cytoolz import curry
from cytoolz.functoolz import compose

@curry
def with_stuff1(arg1, arg2, df):
    return df.withColumn("stuff1", lit(f"{arg1} {arg2}"))

@curry
def with_stuff2(arg, df):
    return df.withColumn("stuff2", lit(arg))
data = [("jose", 1), ("li", 2), ("liz", 3)]
source_df = spark.createDataFrame(data, ["name", "age"])

pipeline = compose(
    with_stuff1("nice", "person"),
    with_stuff2("yoyo")
)
actual_df = pipeline(source_df)
```

```
print(actual_df.show())

+----+---+------+-----------+
|name|age|stuff2|     stuff1|
+----+---+------+-----------+
|jose|  1|  yoyo|nice person|
|  li|  2|  yoyo|nice person|
| liz|  3|  yoyo|nice person|
+----+---+------+-----------+
```

The `compose` function applies transformations from right to left (bottom to top). We can modify the function to apply the transformations from left to right (top to bottom):

```
pipeline = compose(*reversed([
    with_stuff1("nice", "person"),
    with_stuff2("yoyo")
]))
actual_df = pipeline(source_df)
```

```
print(actual_df.show())

+----+---+-----------+------+
|name|age|     stuff1|stuff2|
+----+---+-----------+------+
|jose|  1|nice person|  yoyo|
|  li|  2|nice person|  yoyo|
| liz|  3|nice person|  yoyo|
+----+---+-----------+------+
```

Custom transformations are often order dependent and running them from left to right may be required.

Follow the [best practices outlined in this post](https://mungingdata.com/pyspark/poetry-dependency-management-wheel/) to make it easier to write code with dependencies with cytoolz.

## Custom transformations make testing easier

Custom transformations encourage developers to write code that's easy to test. The code logic is broken up into a bunch of single purpose functions that are easy to understand.

Read [this blog post on testing PySpark code](https://mungingdata.com/pyspark/testing-pytest-chispa/) for examples of how to test custom transformations.

## Chaining custom transformations with the Scala API

The Scala API defines a [Dataset#transform](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) method that makes it easy to chain custom transformations. The Scala programming lanaguage allows for multiple parameter lists, so you don't need to define nested functions.

Chaining custom DataFrame transformations is easier with the Scala API, but still necessary when writing PySpark code!

[This blog post](https://mungingdata.com/apache-spark/chaining-custom-dataframe-transformations/) explains how to chain DataFrame transformations with the Scala API.

## Next steps

You should organize your code as single purpose DataFrame transformations [that are tested individually](https://mungingdata.com/pyspark/testing-pytest-chispa/).

Following [dependency management and project organization best practices](https://mungingdata.com/pyspark/poetry-dependency-management-wheel/) will make your life a lot easier as a PySpark developer. Your development time should be mixed between experimentation in notebooks and coding with software engineering best practices in GitHub repos - both are important.

Use the transform method to chain your DataFrame transformations and run production analyses. Any DataFrame transformations that make assumptions about the underlying schema of a DataFrame should be validated with the [quinn DataFrame validation helper methods](https://github.com/MrPowers/quinn).

If you're writing PySpark code properly, you should be using the transform method quite frequently 😉
