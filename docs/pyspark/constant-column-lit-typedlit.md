---
title: "Adding constant columns with lit and typedLit to PySpark DataFrames"
date: "2021-06-22"
categories: 
  - "pyspark"
---

This post explains how to add constant columns to PySpark DataFrames with `lit` and `typedLit`.

You'll see examples where these functions are useful and when these functions are invoked implicitly.

`lit` and `typedLit` are easy to learn and all PySpark programmers need to be comfortable using them.

## Simple lit example

Create a DataFrame with `num` and `letter` columns.

```
df = spark.createDataFrame([(1, "a"), (2, "b")], ["num", "letter"])
df.show()
```

```
+---+------+
|num|letter|
+---+------+
|  1|     a|
|  2|     b|
+---+------+
```

Add a `cool` column to the DataFrame with the constant value 23.

```
from pyspark.sql.functions import *

df.withColumn("cool", lit(23)).show()
```

```
+---+------+----+
|num|letter|cool|
+---+------+----+
|  1|     a|  23|
|  2|     b|  23|
+---+------+----+
```

Let's try this code without using `lit`:

```
df.withColumn("cool", 23).show()
```

That'll give you the following error:

```
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/dataframe.py", line 2454, in withColumn
    assert isinstance(col, Column), "col should be Column"
AssertionError: col should be Column
```

The second argument to `withColumn` must be a Column object and cannot be an integer.

## Add constant value to column

Let's add 5 to the `num` column:

```
df.withColumn("num_plus_5", df.num + lit(5)).show()
```

```
+---+------+----------+
|num|letter|num_plus_5|
+---+------+----------+
|  1|     a|         6|
|  2|     b|         7|
+---+------+----------+
```

`df.num` and `lit(5)` both return Column objects, as you can observe in the PySpark console.

```
>>> df.num
Column<'num'>
>>> lit(5)
Column<'5'>
```

The `+` operator works when both operands are Column objects.

The `+` operator will also work if one operand is a Column object and the other is an integer.

```
df.withColumn("num_plus_5", df.num + 5).show()
```

```
+---+------+----------+
|num|letter|num_plus_5|
+---+------+----------+
|  1|     a|         6|
|  2|     b|         7|
+---+------+----------+
```

PySpark implicitly converts 5 (an integer) to a Column object and that's why this code works. Let's refresh our understanding of implicit conversions in Python.

## Python type conversions

Let's look at how integers and floating point numbers are added with Python to illustrate the implicit conversion behavior.

An integer cannot be added with a floating point value without type conversion. Language designers either need to throw an error when users add ints and floats or convert the int to a float and then perform the addition. Python language designers made the decision to implicitly convert integers to floating point values in this situation.

Here's an example that uses implicit conversion.

```
3 + 1.2 # 4.2
```

Programmers can also explicitly convert integers to floating point values, so no implicit conversions are needed.

```
float(3) + 1.2 # 4.2
```

Python doesn't always perform implicit type conversions. This code will error out for example:

```
"hi" + 3
```

```
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: can only concatenate str (not "int") to str
```

You need to make an explicit type conversion if you'd like to concatenate a string with an integer in Python.

```
"hi" + str(3) # 'hi3'
```

## PySpark implicit type conversions

Let's look at how PySpark implicitly converts integers to Columns with some console experimentation.

```
# implicit conversion
>>> col("num") + 5
Column<'(num + 5)'>

# explicit conversion
>>> col("num") + lit(5)
Column<'(num + 5)'>
```

It's best to use `lit` and perform explicit conversions, so the intentions of your code are clear. You should avoid relying on implicit conversion rules that may behave unexpectedly in certain situations.

## Array constant column

The Scala API has a `typedLit` function to handle complex types like arrays, but there is no such method in the PySpark API, so hacks are required.

Here's how to add a constant `[5, 8]` array column to the DataFrame.

```
df.withColumn("nums", array(lit(5), lit(8))).show()
```

```
+---+------+------+
|num|letter|  nums|
+---+------+------+
|  1|     a|[5, 8]|
|  2|     b|[5, 8]|
+---+------+------+
```

This code does not work.

```
df.withColumn("nums", lit([5, 8])).show()
```

It errors out as follows:

```
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/functions.py", line 98, in lit
    return col if isinstance(col, Column) else _invoke_function("lit", col)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/functions.py", line 58, in _invoke_function
    return Column(jf(*args))
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/utils.py", line 111, in deco
    return f(*a, **kw)
  File "/Users/powers/spark/spark-3.1.2-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip/py4j/protocol.py", line 326, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling z:org.apache.spark.sql.functions.lit.
: java.lang.RuntimeException: Unsupported literal type class java.util.ArrayList [5, 8]
    at org.apache.spark.sql.catalyst.expressions.Literal$.apply(literals.scala:90)
    at org.apache.spark.sql.catalyst.expressions.Literal$.$anonfun$create$2(literals.scala:152)
    at scala.util.Failure.getOrElse(Try.scala:222)
    at org.apache.spark.sql.catalyst.expressions.Literal$.create(literals.scala:152)
    at org.apache.spark.sql.functions$.typedLit(functions.scala:131)
    at org.apache.spark.sql.functions$.lit(functions.scala:114)
    at org.apache.spark.sql.functions.lit(functions.scala)
    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    at java.lang.reflect.Method.invoke(Method.java:498)
    at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
    at py4j.Gateway.invoke(Gateway.java:282)
    at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    at py4j.commands.CallCommand.execute(CallCommand.java:79)
    at py4j.GatewayConnection.run(GatewayConnection.java:238)
    at java.lang.Thread.run(Thread.java:748)
```

## Next steps

You've learned how to add constant columns to DataFrames in this post. You've also learned about type conversion in PySpark and how the `lit` function is used implicitly in certain situations.

There are times when you can omit `lit` and rely on implicit type conversions, but it's better to write explicit PySpark code and invoke `lit` whenever it's needed.
