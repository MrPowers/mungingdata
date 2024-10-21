---
title: "Creating and reusing the SparkSession with PySpark"
date: "2021-06-19"
categories: 
  - "pyspark"
---

# Creating and reusing the SparkSession with PySpark

This post explains how to create a SparkSession with `getOrCreate` and how to reuse the SparkSession with `getActiveSession`.

You need a SparkSession to read data stored in files, when manually creating DataFrames, and to run arbitrary SQL queries.

The SparkSession should be instantiated once and then reused throughout your application. Most applications should not create multiple sessions or shut down an existing session.

When you're running Spark workflows locally, you're responsible for instantiating the SparkSession yourself. Spark runtime providers build the SparkSession for you and you should reuse it. You need to write code that properly manages the SparkSession for both local and production workflows.

This post shows you how to build a resilient codebase that properly manages the SparkSession in the development, test, and production environments.

## getOrCreate

Here's an example of how to create a SparkSession with the builder:

```
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())
```

`getOrCreate` will either create the SparkSession if one does not already exist or reuse an existing SparkSession.

Let's look at a code snippet from the [chispa](https://github.com/MrPowers/chispa) test suite that uses this SparkSession.

```
import pytest

from spark import *
from chispa import *

def test_assert_df_equality():
    data1 = [(1, "jose"), (2, "li")]
    df1 = spark.createDataFrame(data1, ["num", "name"])
    data2 = [(2, "li"), (1, "jose")]
    df2 = spark.createDataFrame(data2, ["num", "name"])
    assert_df_equality(df1, df2, ignore_row_order=True)
```

`from spark import *` gives us access to the `spark` variable that contains the SparkSession used to create the DataFrames in this test.

Reusing the same SparkSession throughout your test suite is important for your test suite performance. Shutting down and recreating SparkSessions is expensive and causes test suites to run painfully slowly.

## getActiveSession

Some functions can assume a SparkSession exists and should error out if the SparkSession does not exist.

You should only be using `getOrCreate` in functions that should actually be creating a SparkSession. `getActiveSession` is more appropriate for functions that should only reuse an existing SparkSession.

The `show_output_to_df` function in [quinn](https://github.com/MrPowers/quinn) is a good example of a function that uses `getActiveSession`. This function converts the string that's outputted from DataFrame#show back into a DataFrame object. It's useful when you only have the show output in a Stackoverflow question and want to quickly recreate a DataFrame.

Let's take a look at the function in action:

```
import quinn

s = """+----+---+-----------+------+
|name|age|     stuff1|stuff2|
+----+---+-----------+------+
|jose|  1|nice person|  yoyo|
|  li|  2|nice person|  yoyo|
| liz|  3|nice person|  yoyo|
+----+---+-----------+------+"""

df = quinn.show_output_to_df(s)
```

`show_output_to_df` uses a SparkSession under the hood to create the DataFrame, but does not force the user to pass the SparkSession as a function argument because that'd be tedious.

Let's look at the function implementation:

```
def show_output_to_df(show_output):
    l = show_output.split("\n")
    ugly_column_names = l[1]
    pretty_column_names = [i.strip() for i in ugly_column_names[1:-1].split("|")]
    pretty_data = []
    ugly_data = l[3:-1]
    for row in ugly_data:
        r = [i.strip() for i in row[1:-1].split("|")]
        pretty_data.append(tuple(r))
    return SparkSession.getActiveSession().createDataFrame(pretty_data, pretty_column_names)
```

`show_output_to_df` takes a String as an argument and returns a DataFrame. It's a great example of a helper function that hides complexity and makes Spark easier to manage.

## SparkSession from DataFrame

You can also grab the SparkSession that's associated with a DataFrame.

```
data1 = [(1, "jose"), (2, "li")]
df1 = spark.createDataFrame(data1, ["num", "name"])
df1.sql_ctx.sparkSession
```

The SparkSession that's associated with `df1` is the same as the active SparkSession and can also be accessed as follows:

```
from pyspark.sql import SparkSession

SparkSession.getActiveSession()
```

If you have a DataFrame, you can use it to access the SparkSession, but it's best to just grab the SparkSession with `getActiveSession()`.

Let's shut down the active SparkSession to demonstrate the `getActiveSession()` returns `None` when no session exists.

```
spark.stop()

SparkSession.getActiveSession() # None
```

Here's the error you'll get if you try to create a DataFrame now that the SparkSession was stopped.

```
spark.createDataFrame(data1, ["num", "name"])
```

```
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/session.py", line 675, in createDataFrame
    return self._create_dataframe(data, schema, samplingRatio, verifySchema)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/session.py", line 700, in _create_dataframe
    rdd, schema = self._createFromLocal(map(prepare, data), schema)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/session.py", line 526, in _createFromLocal
    return self._sc.parallelize(data), schema
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/context.py", line 530, in parallelize
    numSlices = int(numSlices) if numSlices is not None else self.defaultParallelism
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/context.py", line 442, in defaultParallelism
    return self._jsc.sc().defaultParallelism()
AttributeError: 'NoneType' object has no attribute 'sc'
```

## Next steps

You've learned how to effectively manage the SparkSession in your PySpark applications.

You can create a SparkSession that's reused throughout your test suite and leverage SparkSessions created by third party Spark runtimes.
