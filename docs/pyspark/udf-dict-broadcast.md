---
title: "PySpark UDFs with Dictionary Arguments"
date: "2020-08-08"
categories: 
  - "pyspark"
---

# PySpark UDFs with Dictionary Arguments

Passing a dictionary argument to a PySpark UDF is a powerful programming technique that'll enable you to implement some complicated algorithms that scale.

Broadcasting values and writing UDFs can be tricky. UDFs only accept arguments that are column objects and dictionaries aren't column objects. This blog post shows you the nested function work-around that's necessary for passing a dictionary to a UDF. It'll also show you how to broadcast a dictionary and why broadcasting is important in a cluster environment.

Several approaches that do not work and the accompanying error messages are also presented, so you can learn more about how Spark works.

## You can't pass a dictionary as a UDF argument

Lets create a `state_abbreviation` UDF that takes a string and a dictionary mapping as arguments:

```
@F.udf(returnType=StringType())
def state_abbreviation(s, mapping):
    if s is not None:
        return mapping[s]
```

Create a sample DataFrame, attempt to run the `state_abbreviation` UDF and confirm that the code errors out because UDFs can't take dictionary arguments.

```
import pyspark.sql.functions as F

df = spark.createDataFrame([
    ['Alabama',],
    ['Texas',],
    ['Antioquia',]
]).toDF('state')

mapping = {'Alabama': 'AL', 'Texas': 'TX'}

df.withColumn('state_abbreviation', state_abbreviation(F.col('state'), mapping)).show()
```

Here's the error message: `TypeError: Invalid argument, not a string or column: {'Alabama': 'AL', 'Texas': 'TX'} of type <class 'dict'>. For column literals, use 'lit', 'array', 'struct' or 'create_map' function.`.

The `create_map` function sounds like a promising solution in our case, but that function doesn't help.

Let's see if the `lit` function can help.

```
df.withColumn('state_abbreviation', state_abbreviation(F.col('state'), lit(mapping))).show()
```

This doesn't work either and errors out with this message: `py4j.protocol.Py4JJavaError: An error occurred while calling z:org.apache.spark.sql.functions.lit: java.lang.RuntimeException: Unsupported literal type class java.util.HashMap {Texas=TX, Alabama=AL}`.

The `lit()` function doesn't work with dictionaries.

Let's try broadcasting the dictionary with the `pyspark.sql.functions.broadcast()` method and see if that helps.

```
df.withColumn('state_abbreviation', state_abbreviation(F.col('state'), F.broadcast(mapping))).show()
```

Broadcasting in this manner doesn't help and yields this error message: `AttributeError: 'dict' object has no attribute '_jdf'`.

Broadcasting with `spark.sparkContext.broadcast()` will also error out. You need to approach the problem differently.

## Simple solution

Create a `working_fun` UDF that uses a nested function to avoid passing the dictionary as an argument to the UDF.

```
def working_fun(mapping):
    def f(x):
        return mapping.get(x)
    return F.udf(f)
```

Create a sample DataFrame, run the `working_fun` UDF, and verify the output is accurate.

```
df = spark.createDataFrame([
    ['Alabama',],
    ['Texas',],
    ['Antioquia',]
]).toDF('state')

mapping = {'Alabama': 'AL', 'Texas': 'TX'}

df.withColumn('state_abbreviation', working_fun(mapping)(F.col('state'))).show()
```

```
+---------+------------------+
|    state|state_abbreviation|
+---------+------------------+
|  Alabama|                AL|
|    Texas|                TX|
|Antioquia|              null|
+---------+------------------+
```

This approach works if the dictionary is defined in the codebase (if the dictionary is defined in a Python project that's packaged in a wheel file and attached to a cluster for example). This code will not work in a cluster environment if the dictionary hasn't been spread to all the nodes in the cluster. It's better to explicitly broadcast the dictionary to make sure it'll work when run on a cluster.

## Broadcast solution

Let's refactor `working_fun` by broadcasting the dictionary to all the nodes in the cluster.

```
def working_fun(mapping_broadcasted):
    def f(x):
        return mapping_broadcasted.value.get(x)
    return F.udf(f)

df = spark.createDataFrame([
    ['Alabama',],
    ['Texas',],
    ['Antioquia',]
]).toDF('state')

mapping = {'Alabama': 'AL', 'Texas': 'TX'}
b = spark.sparkContext.broadcast(mapping)

df.withColumn('state_abbreviation', working_fun(b)(F.col('state'))).show()
```

```
+---------+------------------+
|    state|state_abbreviation|
+---------+------------------+
|  Alabama|                AL|
|    Texas|                TX|
|Antioquia|              null|
+---------+------------------+
```

Take note that you need to use `value` to access the dictionary in `mapping_broadcasted.value.get(x)`. If you try to run `mapping_broadcasted.get(x)`, you'll get this error message: `AttributeError: 'Broadcast' object has no attribute 'get'`. You'll see that error message whenever your trying to access a variable that's been broadcasted and forget to call `value`.

Explicitly broadcasting is the best and most reliable way to approach this problem. The dictionary should be explicitly broadcasted, even if it is defined in your code.

## Creating dictionaries to be broadcasted

You'll typically read a dataset from a file, convert it to a dictionary, broadcast the dictionary, and then access the broadcasted variable in your code.

Here's an example code snippet that reads data from a file, converts it to a dictionary, and creates a broadcast variable.

```
df = spark\
    .read\
    .option('header', True)\
    .csv(word_prob_path)
word_prob = {x['word']: x['word_prob'] for x in df.select('word', 'word_prob').collect()}
word_prob_b = spark.sparkContext.broadcast(word_prob)
```

The [quinn](https://github.com/MrPowers/quinn) library makes this even easier.

```
import quinn

word_prob = quinn.two_columns_to_dictionary(df, 'word', 'word_prob')
word_prob_b = spark.sparkContext.broadcast(word_prob)
```

## Broadcast limitations

The broadcast size limit was 2GB and was increased to 8GB as of Spark 2.4, [see here](https://stackoverflow.com/a/58967130/1125159). Big dictionaries can be broadcasted, but you'll need to investigate alternate solutions if that dataset you need to broadcast is truly massive.

## Example application

[wordninja](https://github.com/keredson/wordninja) is a good example of an application that can be easily ported to PySpark with the design pattern outlined in this blog post.

The code depends on an list of 126,000 words defined in [this file](https://github.com/keredson/wordninja/blob/master/wordninja/wordninja_words.txt.gz). The words need to be converted into a dictionary with a key that corresponds to the work and a probability value for the model.

126,000 words sounds like a lot, but it's well below the Spark broadcast limits. You can broadcast a dictionary with millions of key/value pairs.

You can use the design patterns outlined in this blog to run the wordninja algorithm on billions of strings. It's amazing how PySpark lets you scale algorithms!

## Conclusion

Broadcasting dictionaries is a powerful design pattern and oftentimes the key link when porting Python algorithms to PySpark so they can be run at a massive scale.

Your UDF should be packaged in a library that follows [dependency management best practices](https://mungingdata.com/pyspark/poetry-dependency-management-wheel/) and [tested in your test suite](https://mungingdata.com/pyspark/testing-pytest-chispa/). Spark code is complex and following software engineering best practices is essential to build code that's readable and easy to maintain.
